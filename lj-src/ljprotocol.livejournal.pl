#!/usr/bin/perl
#

use strict;
no warnings 'uninitialized';

use LJ::Constants;
use Class::Autouse qw(
                      LJ::Console
                      LJ::Event::JournalNewEntry
                      LJ::Event::UserNewEntry
                      LJ::Event::Befriended
                      LJ::Entry
                      LJ::Poll
                      LJ::EventLogRecord::NewEntry
                      LJ::EventLogRecord::EditEntry
                      LJ::Config
                      LJ::Comment
                      LJ::RateLimit
                      );

LJ::Config->load;

use lib "$ENV{LJHOME}/cgi-bin";

require "taglib.pl";

# have to do this else mailgate will croak with email posting, but only want
# to do it if the site has enabled the hack
require "talklib.pl" if $LJ::NEW_ENTRY_CLEANUP_HACK;

# when posting or editing ping hubbub
require "ljfeed.pl" unless $LJ::DISABLED{'hubbub'};

#### New interface (meta handler) ... other handlers should call into this.
package LJ::Protocol;

# global declaration of this text since we use it in two places
our $CannotBeShown = '(cannot be shown)';

# error classes
use constant E_TEMP => 0;
use constant E_PERM => 1;
# maximum items for get_friends_page function
use constant FRIEND_ITEMS_LIMIT => 50;

my %e = (
     # User Errors
     "100" => [ E_PERM, "Invalid username" ],
     "101" => [ E_PERM, "Invalid password" ],
     "102" => [ E_PERM, "Can't use custom/private security on shared/community journals." ],
     "103" => [ E_PERM, "Poll error" ],
     "104" => [ E_TEMP, "Error adding one or more friends" ],
     "105" => [ E_PERM, "Challenge expired" ],
     "150" => [ E_PERM, "Can't post as non-user" ],
     "151" => [ E_TEMP, "Banned from journal" ],
     "152" => [ E_PERM, "Can't make back-dated entries in non-personal journal." ],
     "153" => [ E_PERM, "Incorrect time value" ],
     "154" => [ E_PERM, "Can't add a redirected account as a friend" ],
     "155" => [ E_TEMP, "Non-authenticated email address" ],
     "156" => [ E_TEMP, sub { # to reload w/o restart
         LJ::tosagree_str('protocol' => 'text') ||
         LJ::tosagree_str('protocol' => 'title')
     } ],
     "157" => [ E_TEMP, "Tags error" ],

     # Client Errors
     "200" => [ E_PERM, "Missing required argument(s)" ],
     "201" => [ E_PERM, "Unknown method" ],
     "202" => [ E_PERM, "Too many arguments" ],
     "203" => [ E_PERM, "Invalid argument(s)" ],
     "204" => [ E_PERM, "Invalid metadata datatype" ],
     "205" => [ E_PERM, "Unknown metadata" ],
     "206" => [ E_PERM, "Invalid destination journal username." ],
     "207" => [ E_PERM, "Protocol version mismatch" ],
     "208" => [ E_PERM, "Invalid text encoding" ],
     "209" => [ E_PERM, "Parameter out of range" ],
     "210" => [ E_PERM, "Client tried to edit with corrupt data.  Preventing." ],
     "211" => [ E_PERM, "Invalid or malformed tag list" ],
     "212" => [ E_PERM, "Message body is too long" ],
     "213" => [ E_PERM, "Message body is empty" ],
     "214" => [ E_PERM, "Message looks like spam" ],


     # Access Errors
     "300" => [ E_TEMP, "Don't have access to requested journal" ],
     "301" => [ E_TEMP, "Access of restricted feature" ],
     "302" => [ E_TEMP, "Can't edit post from requested journal" ],
     "303" => [ E_TEMP, "Can't edit post in community journal" ],
     "304" => [ E_TEMP, "Can't delete post in this community journal" ],
     "305" => [ E_TEMP, "Action forbidden; account is suspended." ],
     "306" => [ E_TEMP, "This journal is temporarily in read-only mode.  Try again in a couple minutes." ],
     "307" => [ E_PERM, "Selected journal no longer exists." ],
     "308" => [ E_TEMP, "Account is locked and cannot be used." ],
     "309" => [ E_PERM, "Account is marked as a memorial." ],
     "310" => [ E_TEMP, "Account needs to be age verified before use." ],
     "311" => [ E_TEMP, "Access temporarily disabled." ],
     "312" => [ E_TEMP, "Not allowed to add tags to entries in this journal" ],
     "313" => [ E_TEMP, "Must use existing tags for entries in this journal (can't create new ones)" ],
     "314" => [ E_PERM, "Only paid users allowed to use this request" ],
     "315" => [ E_PERM, "User messaging is currently disabled" ],
     "316" => [ E_TEMP, "Poster is read-only and cannot post entries." ],
     "317" => [ E_TEMP, "Journal is read-only and entries cannot be posted to it." ],
     "318" => [ E_TEMP, "Poster is read-only and cannot edit entries." ],
     "319" => [ E_TEMP, "Journal is read-only and its entries cannot be edited." ],
     "320" => [ E_TEMP, "Sorry, there was a problem with content of the entry" ],
     "321" => [ E_TEMP, "Sorry, deleting is temporary disabled. Entry is 'private' now" ],

     # Limit errors
     "402" => [ E_TEMP, "Your IP address is temporarily banned for exceeding the login failure rate." ],
     "404" => [ E_TEMP, "Cannot post" ],
     "405" => [ E_TEMP, "Post frequency limit." ],
     "406" => [ E_TEMP, "Client is making repeated requests.  Perhaps it's broken?" ],
     "407" => [ E_TEMP, "Moderation queue full" ],
     "408" => [ E_TEMP, "Maximum queued posts for this community+poster combination reached." ],
     "409" => [ E_PERM, "Post too large." ],
     "410" => [ E_PERM, "Your trial account has expired.  Posting now disabled." ],
     "411" => [ E_TEMP, "Action frequency limit." ],

     # Server Errors
     "500" => [ E_TEMP, "Internal server error" ],
     "501" => [ E_TEMP, "Database error" ],
     "502" => [ E_TEMP, "Database temporarily unavailable" ],
     "503" => [ E_TEMP, "Error obtaining necessary database lock" ],
     "504" => [ E_PERM, "Protocol mode no longer supported." ],
     "505" => [ E_TEMP, "Account data format on server is old and needs to be upgraded." ], # cluster0
     "506" => [ E_TEMP, "Journal sync temporarily unavailable." ],
);

sub translate
{
    my ($u, $msg, $vars) = @_;

    LJ::load_user_props($u, "browselang") unless $u->{'browselang'};
    return LJ::Lang::get_text($u->{'browselang'}, "protocol.$msg", undef, $vars);
}

sub error_class
{
    my $code = shift;
    $code = $1 if $code =~ /^(\d\d\d):(.+)/;
    return $e{$code} && ref $e{$code} ? $e{$code}->[0] : undef;
}

sub error_is_transient
{
    my $class = error_class($_[0]);
    return defined $class ? ! $class+0 : undef;
}

sub error_is_permanent
{
    return error_class($_[0]);
}

sub error_message
{
    my $code = shift;
    my $des;
    ($code, $des) = ($1, $2) if $code =~ /^(\d\d\d):(.+)/;

    my $prefix = "";
    my $error =
      $e{$code} && ref $e{$code}
      ? ( ref $e{$code}->[1] eq 'CODE' ? $e{$code}->[1]->() : $e{$code}->[1] )
      : "BUG: Unknown error code!";
    $prefix = "Client error: " if $code >= 200;
    $prefix = "Server error: " if $code >= 500;
    my $totalerror = "$prefix$error";
    $totalerror .= ": $des" if $des;
    return $totalerror;
}

sub do_request
{
    # get the request and response hash refs
    my ($method, $req, $err, $flags) = @_;

    # if version isn't specified explicitly, it's version 0
    if (ref $req eq "HASH") {
        $req->{'ver'} ||= $req->{'version'};
        $req->{'ver'} = 0 unless defined $req->{'ver'};
    }

    $flags ||= {};
    my @args = ($req, $err, $flags);

    my $r = eval { Apache->request };
    $r->notes("codepath" => "protocol.$method")
        if $r && ! $r->notes("codepath");

    if ($method eq "login")            { return login(@args);            }
    if ($method eq "getfriendgroups")  { return getfriendgroups(@args);  }
    if ($method eq "getfriends")       { return getfriends(@args);       }
    if ($method eq "friendof")         { return friendof(@args);         }
    if ($method eq "checkfriends")     { return checkfriends(@args);     }
    if ($method eq "getdaycounts")     { return getdaycounts(@args);     }
    if ($method eq "postevent")        { return postevent(@args);        }
    if ($method eq "editevent")        { return editevent(@args);        }
    if ($method eq "syncitems")        { return syncitems(@args);        }
    if ($method eq "getevents")        { return getevents(@args);        }
    if ($method eq "editfriends")      { return editfriends(@args);      }
    if ($method eq "editfriendgroups") { return editfriendgroups(@args); }
    if ($method eq "consolecommand")   { return consolecommand(@args);   }
    if ($method eq "getchallenge")     { return getchallenge(@args);     }
    if ($method eq "sessiongenerate")  { return sessiongenerate(@args);  }
    if ($method eq "sessionexpire")    { return sessionexpire(@args);    }
    if ($method eq "getusertags")      { return getusertags(@args);      }
    if ($method eq "getfriendspage")   { return getfriendspage(@args);   }
    if ($method eq "getinbox")         { return getinbox(@args);         }
    if ($method eq "sendmessage")      { return sendmessage(@args);      }
    if ($method eq "setmessageread")   { return setmessageread(@args);   }
    if ($method eq "addcomment")       { return addcomment(@args);   }
    if ($method eq 'checksession')     { return checksession(@args);     }
    if ($method eq "getrecentcomments")       { return getrecentcomments(@args);   }

    $r->notes("codepath" => "") if $r;
    return fail($err,201);
}

sub checksession {
    my ($req, $err, $flags) = @_;

    return undef
        unless authenticate($req, $err, $flags);

    my $u = $flags->{'u'};

    my $session = $u->session;

    return {
        username    => $u->username,
        session     => $u->id.":".$session->id.":".$session->auth,
        caps        => $u->caps,
        usejournals => list_usejournals($u),
    }
}


sub addcomment
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{'u'};
    
    my $journal;
    if( $req->{journal} ){
        return fail($err,100) unless LJ::canonical_username($req->{journal});
        $journal = LJ::load_user($req->{journal}) or return fail($err, 100);
        return fail($err,214) 
            if LJ::Talk::Post::require_captcha_test($u, $journal, $req->{body}, $req->{ditemid});
    }else{
        $journal = $u;
    }
    
    # some additional checks
#    return fail($err,314) unless $u->get_cap('paid');
    return fail($err,214) if LJ::Comment->is_text_spam( \ $req->{body} );
    
    # create
    my $comment = LJ::Comment->create(
                        journal      => $journal,
                        ditemid      => $req->{ditemid},
                        parenttalkid => ($req->{parenttalkid} || int($req->{parent} / 256)),

                        poster       => $u,

                        body         => $req->{body},
                        subject      => $req->{subject},

                        props        => { picture_keyword => $req->{prop_picture_keyword} }
                        );

    # OK
    return {
             status      => "OK",
             commentlink => $comment->url,
             dtalkid     => $comment->dtalkid,
             };
}

sub getrecentcomments {
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{'u'};
    my $count = $req->{itemshow};
    $count = 10 if !$count || ($count > 100) || ($count < 0);

    my @recv = $u->get_recent_talkitems($count);
    my @recv_talkids = map { $_->{'jtalkid'} } @recv;
    my %recv_userids = map { $_->{'posterid'} => 1} @recv;
    my $comment_text = LJ::get_talktext2($u, @recv_talkids);
    my $users = LJ::load_userids(keys(%recv_userids));
    foreach my $comment ( @recv ) {
        $comment->{subject} = $comment_text->{$comment->{jtalkid}}[0];
        $comment->{text} = $comment_text->{$comment->{jtalkid}}[1];

        $comment->{text} = LJ::trim_widgets(
            length     => $req->{trim_widgets},
            img_length => $req->{widgets_img_length},
            text      => $comment->{text},
            read_more => '<a href="' . $comment->url . '"> ...</a>',
        ) if $req->{trim_widgets};

        $comment->{text} = LJ::convert_lj_tags_to_links(
            event => $comment->{text},
            embed_url => $comment->url,
        ) if $req->{parseljtags};

        $comment->{postername} = $users->{$comment->{posterid}}
            && $users->{$comment->{posterid}}->username;
    }
    return  { status => 'OK', comments => [ @recv ] };
}


sub getfriendspage
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{'u'};

    my $itemshow = (defined $req->{itemshow}) ? $req->{itemshow} : 100;
    return fail($err, 209, "Bad itemshow value") if $itemshow ne int($itemshow ) or $itemshow  <= 0 or $itemshow  > 100;
    my $skip = (defined $req->{skip}) ? $req->{skip} : 0;
    return fail($err, 209, "Bad skip value") if $skip ne int($skip ) or $skip  < 0 or $skip  > 100;

    my $lastsync = int $req->{lastsync};
    my $before = int $req->{before};
    my $before_count = 0;
    my $before_skip = 0;
    if ($before){
        $before_skip = $skip + 0;
        $skip = 0;
    }
    
    my @entries = LJ::get_friend_items({
        'u' => $u,
        'userid' => $u->{'userid'},
        'remote' => $u,
        'itemshow' => $itemshow,
        'skip' => $skip,
        'dateformat' => 'S2',
    });

    my @attrs = qw/subject_raw event_raw journalid posterid ditemid security reply_count userpic/;

    my @uids;

    my @res = ();
    while (my $ei = shift @entries) {

        next unless $ei;

        # exit cycle if maximum friend items limit reached
        last
            if scalar @res >= FRIEND_ITEMS_LIMIT;

        # if passed lastsync argument - skip items with logtime less than lastsync
        if($lastsync) {
            next
                if $LJ::EndOfTime - $ei->{rlogtime} <= $lastsync;
        }

        if($before) {
            last if @res >= $itemshow;
            push @entries, LJ::get_friend_items({
                'u' => $u,
                'userid' => $u->{'userid'},
                'remote' => $u,
                'itemshow' => $itemshow,
                'skip' => $skip + ($before_count += $itemshow),
                'dateformat' => 'S2',
            }) unless @entries;
            next if $LJ::EndOfTime - $ei->{rlogtime} > $before;
            next if $before_skip-- > 0;
        }

        my $entry = LJ::Entry->new_from_item_hash($ei);
        next unless $entry;

        # event result data structure
        my %h = ();

        # Add more data for public posts
        foreach my $method (@attrs) {
            $h{$method} = $entry->$method;
        }

        $h{event_raw} = LJ::trim_widgets(
            length    => $req->{trim_widgets},
            img_length => $req->{widgets_img_length},
            text      => $h{event_raw},
            read_more => '<a href="' . $entry->url . '"> ...</a>',
        ) if $req->{trim_widgets};

        $h{event_raw} = LJ::convert_lj_tags_to_links(
            event => $h{event_raw},
            embed_url => $entry->url,
        ) if $req->{parseljtags};
        
        #userpic 
        $h{poster_userpic_url} = $h{userpic} && $h{userpic}->url;
        
        # log time value
        $h{logtime} = $LJ::EndOfTime - $ei->{rlogtime};
        $h{do_captcha} = LJ::Talk::Post::require_captcha_test($u, $entry->poster, '', $h{ditemid})?1:0;

        push @res, \%h;

        push @uids, $h{posterid}, $h{journalid};
    }

    my $users = LJ::load_userids(@uids);

    foreach (@res) {
        $_->{journalname} = $users->{ $_->{journalid} }->{'user'};
        $_->{journaltype} = $users->{ $_->{journalid} }->{'journaltype'};
        $_->{journalurl}  = $users->{ $_->{journalid} }->journal_base;
        delete $_->{journalid};
        $_->{postername} = $users->{ $_->{posterid} }->{'user'};
        $_->{postertype} = $users->{ $_->{posterid} }->{'journaltype'};
        $_->{posterurl}  = $users->{ $_->{posterid} }->journal_base;
        delete $_->{posterid};
    }

    LJ::run_hooks("getfriendspage", { 'userid' => $u->userid, });

    return { entries => [ @res ], skip => $skip };
}

sub getinbox
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{'u'};

    my $itemshow = (defined $req->{itemshow}) ? $req->{itemshow} : 100;
    return fail($err, 209, "Bad itemshow value") if $itemshow ne int($itemshow ) or $itemshow  <= 0 or $itemshow  > 100;
    my $skip = (defined $req->{skip}) ? $req->{skip} : 0;
    return fail($err, 209, "Bad skip value") if $skip ne int($skip ) or $skip  < 0 or $skip  > 100;

    # get the user's inbox
    my $inbox = $u->notification_inbox or return fail($err, 500, "Cannot get user inbox");

    my %type_number = (
        Befriended           => 1,
        Birthday             => 2,
        CommunityInvite      => 3,
        CommunityJoinApprove => 4,
        CommunityJoinReject  => 5,
        CommunityJoinRequest => 6,
        Defriended           => 7,
        InvitedFriendJoins   => 8,
        JournalNewComment    => 9,
        JournalNewEntry      => 10,
        NewUserpic           => 11,
        NewVGift             => 12,
        OfficialPost         => 13,
        PermSale             => 14,
        PollVote             => 15,
        SupOfficialPost      => 16,
        UserExpunged         => 17,
        UserMessageRecvd     => 18,
        UserMessageSent      => 19,
        UserNewComment       => 20,
        UserNewEntry         => 21,
    );
    my %number_type = reverse %type_number;

    my @notifications;

    my $sync_date;
    # check lastsync for valid date
    if ($req->{'lastsync'}) {
        $sync_date = int $req->{'lastsync'};
        if($sync_date <= 0) {
            return fail($err,203,"Invalid syncitems date format (must be unixtime)");
        }
    }

    if ($req->{gettype}) {
        $req->{gettype} = [$req->{gettype}] unless ref($req->{gettype});
        
        my %filter;
        $filter{"LJ::Event::" . $number_type{$_}} = 1 for @{$req->{gettype}};
        @notifications = grep { exists $filter{$_->event->class} } $inbox->items;
        
    } else {
        @notifications = $inbox->all_items;
    }

    # By default, notifications are sorted as "oldest are the first"
    # Reverse it by "newest are the first"
    @notifications = reverse @notifications;

    if (my $before = $req->{'before'}) {
        return fail($err,203,"Invalid syncitems date format (must be unixtime)") if $before <= 0;
        @notifications = grep {$_->when_unixtime <= $before} @notifications;
    }

    $itemshow = scalar @notifications - $skip if scalar @notifications < $skip + $itemshow;

    my @res;
    foreach my $item (@notifications[$skip .. $itemshow + $skip - 1]) {
        next if $sync_date && $item->when_unixtime < $sync_date;

        my $raw = $item->event->raw_info($u, {extended => $req->{extended}});

        my $type_index = $type_number{$raw->{type}};
        if (defined $type_index) {
            $raw->{type} = $type_index;
        } else {
            $raw->{typename} = $raw->{type};
            $raw->{type} = 0;
        }

        $raw->{state} = $item->{state};

        push @res, { %$raw,
                     when   => $item->when_unixtime,
                     qid    => $item->qid,
                   };
    }

    return { 'skip'  => $skip,
             'items' => \@res,
             'login' => $u->user,
             'journaltype' => $u->journaltype };
}

sub setmessageread {
    my ($req, $err, $flags) = @_;

    return undef unless authenticate($req, $err, $flags);

    my $u = $flags->{'u'};

    # get the user's inbox
    my $inbox = $u->notification_inbox or return fail($err, 500, "Cannot get user inbox");
    my @result;

    # passing requested ids for loading
    my @notifications = $inbox->all_items;

    # Try to select messages by qid if specified
    my @qids = @{$req->{qid}};
    if (scalar @qids) {
        foreach my $qid (@qids) {
            my $item = eval {LJ::NotificationItem->new($u, $qid)};
            $item->mark_read if $item;
            push @result, { qid => $qid, result => 'set read'  };
        }
    } else { # Else select it by msgid for back compatibility
        # make hash of requested message ids
        my %requested_items = map { $_ => 1 } @{$req->{messageid}};

        # proccessing only requested ids
        foreach my $item (@notifications) {
            my $msgid = $item->event->raw_info($u)->{msgid};
            next unless $requested_items{$msgid};
            # if message already read -
            if ($item->{state} eq 'R') {
                push @result, { msgid => $msgid, result => 'already red' };
                next;
            }
            # in state no 'R' - marking as red
            $item->mark_read;
            push @result, { msgid => $msgid, result => 'set read'  };
        }
    }

    return {
        result => \@result
    };

}

sub sendmessage
{
    my ($req, $err, $flags) = @_;

    return fail($err, 315) if $LJ::DISABLED{user_messaging};

    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{'u'};

    return fail($err, 305) if $u->statusvis eq 'S'; # suspended cannot send private messages

    my $msg_limit = LJ::get_cap($u, "usermessage_length");

    my @errors;

    my $subject_text = LJ::strip_html($req->{'subject'});
    return fail($err, 208, 'subject')
        unless LJ::text_in($subject_text);

    # strip HTML from body and test encoding and length
    my $body_text = LJ::strip_html($req->{'body'});
    return fail($err, 208, 'body')
        unless LJ::text_in($body_text);

    my ($msg_len_b, $msg_len_c) = LJ::text_length($body_text);
    return fail($err, 212, 'found: ' . LJ::commafy($msg_len_c) . ' characters, it should not exceed ' . LJ::commafy($msg_limit))
        unless ($msg_len_c <= $msg_limit);


    return fail($err, 213, 'found: ' . LJ::commafy($msg_len_c) . ' characters, it should exceed zero')
        if ($msg_len_c <= 0);

    my @to = (ref $req->{'to'}) ? @{$req->{'to'}} : ($req->{'to'});
    return fail($err, 200) unless scalar @to;

    # remove duplicates
    my %to = map { lc($_), 1 } @to;
    @to = keys %to;

    my @msg;
    BML::set_language('en'); # FIXME

    foreach my $to (@to) {
        my $tou = LJ::load_user($to);
        return fail($err, 100, $to)
            unless $tou;

        my $msg = LJ::Message->new({
                    journalid => $u->userid,
                    otherid => $tou->userid,
                    subject => $subject_text,
                    body => $body_text,
                    parent_msgid => defined $req->{'parent'} ? $req->{'parent'} + 0 : undef,
                    userpic => $req->{'userpic'} || undef,
                  });

        push @msg, $msg
            if $msg->can_send(\@errors);
    }
    return fail($err, 203, join('; ', @errors))
        if scalar @errors;

    foreach my $msg (@msg) {
        $msg->send(\@errors);
    }

    return { 'sent_count' => scalar @msg, 'msgid' => [ grep { $_ } map { $_->msgid } @msg ],
             (@errors ? ('last_errors' => \@errors) : () ),
           };
}

sub login
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);

    my $u = $flags->{'u'};
    my $res = {};
    my $ver = $req->{'ver'};

    ## check for version mismatches
    ## non-Unicode installations can't handle versions >=1

    return fail($err,207, "This installation does not support Unicode clients")
        if $ver>=1 and not $LJ::UNICODE;

    # do not let locked people log in
    return fail($err, 308) if $u->{statusvis} eq 'L';

    ## return a message to the client to be displayed (optional)
    login_message($req, $res, $flags);
    LJ::text_out(\$res->{'message'}) if $ver>=1 and defined $res->{'message'};

    ## report what shared journals this user may post in
    $res->{'usejournals'} = list_usejournals($u);

    ## return their friend groups
    $res->{'friendgroups'} = list_friendgroups($u);
    return fail($err, 502, "Error loading friend groups") unless $res->{'friendgroups'};
    if ($ver >= 1) {
        foreach (@{$res->{'friendgroups'}}) {
            LJ::text_out(\$_->{'name'});
        }
    }

    ## if they gave us a number of moods to get higher than, then return them
    if (defined $req->{'getmoods'}) {
        $res->{'moods'} = list_moods($req->{'getmoods'});
        if ($ver >= 1) {
            # currently all moods are in English, but this might change
            foreach (@{$res->{'moods'}}) { LJ::text_out(\$_->{'name'}) }
        }
    }

    ### picture keywords, if they asked for them.
    if ($req->{'getpickws'} || $req->{'getpickwurls'}) {
        my $pickws = list_pickws($u);
        @$pickws = sort { lc($a->[0]) cmp lc($b->[0]) } @$pickws;
        $res->{'pickws'} = [ map { $_->[0] } @$pickws ] if $req->{'getpickws'};
        if ($req->{'getpickwurls'}) {
            if ($u->{'defaultpicid'}) {
                 $res->{'defaultpicurl'} = "$LJ::USERPIC_ROOT/$u->{'defaultpicid'}/$u->{'userid'}";
            }
            $res->{'pickwurls'} = [ map {
                "$LJ::USERPIC_ROOT/$_->[1]/$u->{'userid'}"
            } @$pickws ];
        }
        if ($ver >= 1) {
            # validate all text
            foreach(@{$res->{'pickws'}}) { LJ::text_out(\$_); }
            foreach(@{$res->{'pickwurls'}}) { LJ::text_out(\$_); }
            LJ::text_out(\$res->{'defaultpicurl'});
        }
    }
    ## return caps, if they asked for them
    if ($req->{'getcaps'}) {
        $res->{'caps'} = $u->caps;
    }

    ## return client menu tree, if requested
    if ($req->{'getmenus'}) {
        $res->{'menus'} = hash_menus($u);
        if ($ver >= 1) {
            # validate all text, just in case, even though currently
            # it's all English
            foreach (@{$res->{'menus'}}) {
                LJ::text_out(\$_->{'text'});
                LJ::text_out(\$_->{'url'}); # should be redundant
            }
        }
    }

    ## tell some users they can hit the fast servers later.
    $res->{'fastserver'} = 1 if LJ::get_cap($u, "fastserver");

    ## user info
    $res->{'userid'} = $u->{'userid'};
    $res->{'fullname'} = $u->{'name'};
    LJ::text_out(\$res->{'fullname'}) if $ver >= 1;

    if ($req->{'clientversion'} =~ /^\S+\/\S+$/) {
        eval {
            my $r = Apache->request;
            $r->notes("clientver", $req->{'clientversion'});
        };
    }

    ## update or add to clientusage table
    if ($req->{'clientversion'} =~ /^\S+\/\S+$/ &&
        ! $LJ::DISABLED{'clientversionlog'})
    {
        my $client = $req->{'clientversion'};

        return fail($err, 208, "Bad clientversion string")
            if $ver >= 1 and not LJ::text_in($client);

        my $dbh = LJ::get_db_writer();
        my $qclient = $dbh->quote($client);
        my $cu_sql = "REPLACE INTO clientusage (userid, clientid, lastlogin) " .
            "SELECT $u->{'userid'}, clientid, NOW() FROM clients WHERE client=$qclient";
        my $sth = $dbh->prepare($cu_sql);
        $sth->execute;
        unless ($sth->rows) {
            # only way this can be 0 is if client doesn't exist in clients table, so
            # we need to add a new row there, to get a new clientid for this new client:
            $dbh->do("INSERT INTO clients (client) VALUES ($qclient)");
            # and now we can do the query from before and it should work:
            $sth = $dbh->prepare($cu_sql);
            $sth->execute;
        }
    }

    return $res;
}

sub getfriendgroups
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{'u'};
    my $res = {};
    $res->{'friendgroups'} = list_friendgroups($u);
    return fail($err, 502, "Error loading friend groups") unless $res->{'friendgroups'};
    if ($req->{'ver'} >= 1) {
        foreach (@{$res->{'friendgroups'} || []}) {
            LJ::text_out(\$_->{'name'});
        }
    }
    return $res;
}

sub getusertags
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    return undef unless check_altusage($req, $err, $flags);

    my $u = $flags->{'u'};
    my $uowner = $flags->{'u_owner'} || $u;
    return fail($req, 502) unless $u && $uowner;

    my $tags = LJ::Tags::get_usertags($uowner, { remote => $u });
    return { tags => [ values %$tags ] };
}

sub getfriends
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    return fail($req,502) unless LJ::get_db_reader();
    my $u = $flags->{'u'};
    my $res = {};
    if ($req->{'includegroups'}) {
        $res->{'friendgroups'} = list_friendgroups($u);
        return fail($err, 502, "Error loading friend groups") unless $res->{'friendgroups'};
        if ($req->{'ver'} >= 1) {
            foreach (@{$res->{'friendgroups'} || []}) {
                LJ::text_out(\$_->{'name'});
            }
        }
    }
    # TAG:FR:protocol:getfriends_of
    if ($req->{'includefriendof'}) {
        $res->{'friendofs'} = list_friends($u, {
            'limit' => $req->{'friendoflimit'},
            'friendof' => 1,
        });
        if ($req->{'ver'} >= 1) {
            foreach(@{$res->{'friendofs'}}) { LJ::text_out(\$_->{'fullname'}) };
        }
    }
    # TAG:FR:protocol:getfriends
    $res->{'friends'} = list_friends($u, {
        'limit' => $req->{'friendlimit'},
        'includebdays' => $req->{'includebdays'},
    });
    if ($req->{'ver'} >= 1) {
        foreach(@{$res->{'friends'}}) { LJ::text_out(\$_->{'fullname'}) };
    }
    return $res;
}

sub friendof
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    return fail($req,502) unless LJ::get_db_reader();
    my $u = $flags->{'u'};
    my $res = {};

    # TAG:FR:protocol:getfriends_of2 (same as TAG:FR:protocol:getfriends_of)
    $res->{'friendofs'} = list_friends($u, {
        'friendof' => 1,
        'limit' => $req->{'friendoflimit'},
    });
    if ($req->{'ver'} >= 1) {
        foreach(@{$res->{'friendofs'}}) { LJ::text_out(\$_->{'fullname'}) };
    }
    return $res;
}

sub checkfriends
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{'u'};
    my $res = {};

    # return immediately if they can't use this mode
    unless (LJ::get_cap($u, "checkfriends")) {
        $res->{'new'} = 0;
        $res->{'interval'} = 36000;  # tell client to bugger off
        return $res;
    }

    ## have a valid date?
    my $lastupdate = $req->{'lastupdate'};
    if ($lastupdate) {
        return fail($err,203) unless
            ($lastupdate =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d/);
    } else {
        $lastupdate = "0000-00-00 00:00:00";
    }

    my $interval = LJ::get_cap_min($u, "checkfriends_interval");
    $res->{'interval'} = $interval;

    my $mask;
    if ($req->{'mask'} and $req->{'mask'} !~ /\D/) {
        $mask = $req->{'mask'};
    }

    my $memkey = [$u->{'userid'},"checkfriends:$u->{userid}:$mask"];
    my $update = LJ::MemCache::get($memkey);
    unless ($update) {
        # TAG:FR:protocol:checkfriends (wants reading list of mask, not "friends")
        my $fr = LJ::get_friends($u, $mask);
        unless ($fr && %$fr) {
            $res->{'new'} = 0;
            $res->{'lastupdate'} = $lastupdate;
            return $res;
        }
        if (@LJ::MEMCACHE_SERVERS) {
            my $tu = LJ::get_timeupdate_multi({ memcache_only => 1 }, keys %$fr);
            my $max = 0;
            while ($_ = each %$tu) {
                $max = $tu->{$_} if $tu->{$_} > $max;
            }
            $update = LJ::mysql_time($max) if $max;
        } else {
            my $dbr = LJ::get_db_reader();
            unless ($dbr) {
                # rather than return a 502 no-db error, just say no updates,
                # because problem'll be fixed soon enough by db admins
                $res->{'new'} = 0;
                $res->{'lastupdate'} = $lastupdate;
                return $res;
            }
            my $list = join(", ", map { int($_) } keys %$fr);
            if ($list) {
              my $sql = "SELECT MAX(timeupdate) FROM userusage ".
                  "WHERE userid IN ($list)";
              $update = $dbr->selectrow_array($sql);
            }
        }
        LJ::MemCache::set($memkey,$update,time()+$interval) if $update;
    }
    $update ||= "0000-00-00 00:00:00";

    if ($req->{'lastupdate'} && $update gt $lastupdate) {
        $res->{'new'} = 1;
    } else {
        $res->{'new'} = 0;
    }

    $res->{'lastupdate'} = $update;
    return $res;
}

sub getdaycounts
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    return undef unless check_altusage($req, $err, $flags);

    my $u = $flags->{'u'};
    my $uowner = $flags->{'u_owner'} || $u;
    my $ownerid = $flags->{'ownerid'};

    my $res = {};
    my $daycts = LJ::get_daycounts($uowner, $u);
    return fail($err,502) unless $daycts;

    foreach my $day (@$daycts) {
        my $date = sprintf("%04d-%02d-%02d", $day->[0], $day->[1], $day->[2]);
        push @{$res->{'daycounts'}}, { 'date' => $date, 'count' => $day->[3] };
    }
    return $res;
}

sub common_event_validation
{
    my ($req, $err, $flags) = @_;

    # clean up event whitespace
    # remove surrounding whitespace
    $req->{event} =~ s/^\s+//;
    $req->{event} =~ s/\s+$//;

    # convert line endings to unix format
    if ($req->{'lineendings'} eq "mac") {
        $req->{event} =~ s/\r/\n/g;
    } else {
        $req->{event} =~ s/\r//g;
    }

    # date validation
    if ($req->{'year'} !~ /^\d\d\d\d$/ ||
        $req->{'year'} < 1970 ||    # before unix time started = bad
        $req->{'year'} > 2037)      # after unix time ends = worse!  :)
    {
        return fail($err,203,"Invalid year value.");
    }
    if ($req->{'mon'} !~ /^\d{1,2}$/ ||
        $req->{'mon'} < 1 ||
        $req->{'mon'} > 12)
    {
        return fail($err,203,"Invalid month value.");
    }
    if ($req->{'day'} !~ /^\d{1,2}$/ || $req->{'day'} < 1 ||
        $req->{'day'} > LJ::days_in_month($req->{'mon'},
                                          $req->{'year'}))
    {
        return fail($err,203,"Invalid day of month value.");
    }
    if ($req->{'hour'} !~ /^\d{1,2}$/ ||
        $req->{'hour'} < 0 || $req->{'hour'} > 23)
    {
        return fail($err,203,"Invalid hour value.");
    }
    if ($req->{'min'} !~ /^\d{1,2}$/ ||
        $req->{'min'} < 0 || $req->{'min'} > 59)
    {
        return fail($err,203,"Invalid minute value.");
    }

    # column width
    # we only trim Unicode data

    if ($req->{'ver'} >=1 ) {
        $req->{'subject'} = LJ::text_trim($req->{'subject'}, LJ::BMAX_SUBJECT, LJ::CMAX_SUBJECT);
        $req->{'event'} = LJ::text_trim($req->{'event'}, LJ::BMAX_EVENT, LJ::CMAX_EVENT);
        foreach (keys %{$req->{'props'}}) {
            # do not trim this property, as it's magical and handled later
            next if $_ eq 'taglist';

            # Allow syn_links and syn_ids the full width of the prop, to avoid truncating long URLS
            if ($_ eq 'syn_link' || $_ eq 'syn_id') {
                $req->{'props'}->{$_} = LJ::text_trim($req->{'props'}->{$_}, LJ::BMAX_PROP);
            } else {
                $req->{'props'}->{$_} = LJ::text_trim($req->{'props'}->{$_}, LJ::BMAX_PROP, LJ::CMAX_PROP);
            }

        }
    }

    # setup non-user meta-data.  it's important we define this here to
    # 0.  if it's not defined at all, then an editevent where a user
    # removes random 8bit data won't remove the metadata.  not that
    # that matters much.  but having this here won't hurt.  false
    # meta-data isn't saved anyway.  so the only point of this next
    # line is making the metadata be deleted on edit.
    $req->{'props'}->{'unknown8bit'} = 0;

    # we don't want attackers sending something that looks like gzipped data
    # in protocol version 0 (unknown8bit allowed), otherwise they might
    # inject a 100MB string of single letters in a few bytes.
    return fail($err,208,"Cannot send gzipped data")
        if substr($req->{'event'},0,2) eq "\037\213";

    # non-ASCII?
    unless ( $flags->{'use_old_content'} || (
        LJ::is_ascii($req->{'event'}) &&
        LJ::is_ascii($req->{'subject'}) &&
        LJ::is_ascii(join(' ', values %{$req->{'props'}})) ))
    {

        if ($req->{'ver'} < 1) { # client doesn't support Unicode
            # only people should have unknown8bit entries.
            my $uowner = $flags->{u_owner} || $flags->{u};
            return fail($err,207,'Posting in a community with international or special characters require a Unicode-capable LiveJournal client.  Download one at http://www.livejournal.com/download/.')
                if $uowner->{journaltype} ne 'P';

            # so rest of site can change chars to ? marks until
            # default user's encoding is set.  (legacy support)
            $req->{'props'}->{'unknown8bit'} = 1;
        } else {
            return fail($err,207, "This installation does not support Unicode clients") unless $LJ::UNICODE;
            # validate that the text is valid UTF-8
            if (!LJ::text_in($req->{'subject'}) ||
                !LJ::text_in($req->{'event'}) ||
                grep { !LJ::text_in($_) } values %{$req->{'props'}}) {
                return fail($err, 208, "The text entered is not a valid UTF-8 stream");
            }
        }
    }

    ## handle meta-data (properties)
    LJ::load_props("log");
    foreach my $pname (keys %{$req->{'props'}})
    {
        my $p = LJ::get_prop("log", $pname);

        # does the property even exist?
        unless ($p) {
            $pname =~ s/[^\w]//g;
            return fail($err,205,$pname);
        }

        # don't validate its type if it's 0 or undef (deleting)
        next unless ($req->{'props'}->{$pname});

        my $ptype = $p->{'datatype'};
        my $val = $req->{'props'}->{$pname};

        if ($ptype eq "bool" && $val !~ /^[01]$/) {
            return fail($err,204,"Property \"$pname\" should be 0 or 1");
        }
        if ($ptype eq "num" && $val =~ /[^\d]/) {
            return fail($err,204,"Property \"$pname\" should be numeric");
        }
        if ($pname eq "current_coords" && ! eval { LJ::Location->new(coords => $val) }) {
            return fail($err,204,"Property \"current_coords\" has invalid value");
        }
    }

    # check props for inactive userpic
    if (my $pickwd = $req->{'props'}->{'picture_keyword'}) {
        my $pic = LJ::get_pic_from_keyword($flags->{'u'}, $pickwd);

        # need to make sure they aren't trying to post with an inactive keyword, but also
        # we don't want to allow them to post with a keyword that has no pic at all to prevent
        # them from deleting the keyword, posting, then adding it back with editpics.bml
        delete $req->{'props'}->{'picture_keyword'} if ! $pic || $pic->{'state'} eq 'I';
    }

    # validate incoming list of tags
    return fail($err, 211)
        if $req->{props}->{taglist} &&
           ! LJ::Tags::is_valid_tagstring($req->{props}->{taglist});

    return 1;
}

sub postevent
{
    my ($req, $err, $flags) = @_;
    un_utf8_request($req);

    my $post_noauth = LJ::run_hook('post_noauth', $req);
    return undef unless $post_noauth || authenticate($req, $err, $flags);

    my $spam = 0;
    LJ::run_hook('spam_detector', $req, \$spam);
    return fail($err,320) if $spam;

    # if going through mod queue, then we know they're permitted to post at least this entry
    $flags->{'usejournal_okay'} = 1 if $post_noauth;
    return undef unless check_altusage($req, $err, $flags) || $flags->{nomod};

    my $u = $flags->{'u'};
    my $ownerid = $flags->{'ownerid'}+0;
    my $uowner = $flags->{'u_owner'} || $u;
    # Make sure we have a real user object here
    $uowner = LJ::want_user($uowner) unless LJ::isu($uowner);
    my $clusterid = $uowner->{'clusterid'};

    my $dbh = LJ::get_db_writer();
    my $dbcm = LJ::get_cluster_master($uowner);

    return fail($err,306) unless $dbh && $dbcm && $uowner->writer;
    return fail($err,200) unless $req->{'event'} =~ /\S/;

    ### make sure community, shared, or news journals don't post
    ### note: shared and news journals are deprecated.  every shared journal
    ##        should one day be a community journal, of some form.
    return fail($err,150) if ($u->{'journaltype'} eq "C" ||
                              $u->{'journaltype'} eq "S" ||
                              $u->{'journaltype'} eq "I" ||
                              $u->{'journaltype'} eq "N");

    # underage users can't do this
    return fail($err,310) if $u->underage;

    # suspended users can't post
    return fail($err,305) if ($u->{'statusvis'} eq "S");

    # memorials can't post
    return fail($err,309) if $u->{statusvis} eq 'M';

    # locked accounts can't post
    return fail($err,308) if $u->{statusvis} eq 'L';

    # check the journal's read-only bit
    return fail($err,306) if LJ::get_cap($uowner, "readonly");

    # is the user allowed to post?
    return fail($err,404,$LJ::MSG_NO_POST) unless LJ::get_cap($u, "can_post");

    # is the user allowed to post?
    return fail($err,410) if LJ::get_cap($u, "disable_can_post");

    # read-only accounts can't post
    return fail($err,316) if $u->is_readonly;

    # read-only accounts can't be posted to
    return fail($err,317) if $uowner->is_readonly;

    # can't post to deleted/suspended community
    return fail($err,307) unless $uowner->{'statusvis'} eq "V";

    # user must have a validated email address to post to any journal - including its own,
    # except syndicated (rss, 'Y') journals
    # unless this is approved from the mod queue (we'll error out initially, but in case they change later)
    return fail($err, 155, "You must have an authenticated email address in order to post to another account")
        unless $u->{'status'} eq 'A' || $u->{'journaltype'} eq 'Y';

    # post content too large
    # NOTE: requires $req->{event} be binary data, but we've already
    # removed the utf-8 flag in the XML-RPC path, and it never gets
    # set in the "flat" protocol path.
    return fail($err,409) if length($req->{'event'}) >= LJ::BMAX_EVENT;

    my $time_was_faked = 0;
    my $offset = 0;  # assume gmt at first.

    if (defined $req->{'tz'}) {
        if ($req->{tz} eq 'guess') {
            LJ::get_timezone($u, \$offset, \$time_was_faked);
        } elsif ($req->{'tz'} =~ /^[+\-]\d\d\d\d$/) {
            # FIXME we ought to store this timezone and make use of it somehow.
            $offset = $req->{'tz'} / 100.0;
        } else {
            return fail($err, 203, "Invalid tz");
        }
    }

    if (defined $req->{'tz'} and not grep { defined $req->{$_} } qw(year mon day hour min)) {
        my @ltime = gmtime(time() + ($offset*3600));
        $req->{'year'} = $ltime[5]+1900;
        $req->{'mon'}  = $ltime[4]+1;
        $req->{'day'}  = $ltime[3];
        $req->{'hour'} = $ltime[2];
        $req->{'min'}  = $ltime[1];
        $time_was_faked = 1;
    }

    return undef
        unless common_event_validation($req, $err, $flags);

    # confirm we can add tags, at least
    return fail($err, 312)
        if $req->{props} && $req->{props}->{taglist} &&
           ! LJ::Tags::can_add_tags($uowner, $u);

    my $event = $req->{'event'};

    ### allow for posting to journals that aren't yours (if you have permission)
    my $posterid = $u->{'userid'}+0;

    # make the proper date format
    my $eventtime = sprintf("%04d-%02d-%02d %02d:%02d",
                                $req->{'year'}, $req->{'mon'},
                                $req->{'day'}, $req->{'hour'},
                                $req->{'min'});
    my $qeventtime = $dbh->quote($eventtime);

    # load userprops all at once
    my @poster_props = qw(newesteventtime dupsig_post);
    my @owner_props = qw(newpost_minsecurity moderated);
    push @owner_props, 'opt_weblogscom' unless $req->{'props'}->{'opt_backdated'};

    LJ::load_user_props($u, @poster_props, @owner_props);
    if ($uowner->{'userid'} == $u->{'userid'}) {
        $uowner->{$_} = $u->{$_} foreach (@owner_props);
    } else {
        LJ::load_user_props($uowner, @owner_props);
    }

    # are they trying to post back in time?
    if ($posterid == $ownerid && $u->{'journaltype'} ne 'Y' &&
        !$time_was_faked && $u->{'newesteventtime'} &&
        $eventtime lt $u->{'newesteventtime'} &&
        !$req->{'props'}->{'opt_backdated'}) {
        return fail($err, 153, "You have an entry which was posted at $u->{'newesteventtime'}, but you're trying to post an entry before this. Please check the date and time of both entries. If the other entry is set in the future on purpose, edit that entry to use the \"Date Out of Order\" option. Otherwise, use the \"Date Out of Order\" option for this entry instead.");
    }

    my $qallowmask = $req->{'allowmask'}+0;
    my $security = "public";
    my $uselogsec = 0;
    if ($req->{'security'} eq "usemask" || $req->{'security'} eq "private") {
        $security = $req->{'security'};
    }
    if ($req->{'security'} eq "usemask") {
        $uselogsec = 1;
    }

    # can't specify both a custom security and 'friends-only'
    return fail($err, 203, "Invalid friends group security set")
        if $qallowmask > 1 && $qallowmask % 2;

    ## if newpost_minsecurity is set, new entries have to be
    ## a minimum security level
    $security = "private"
        if $uowner->newpost_minsecurity eq "private";
    ($security, $qallowmask) = ("usemask", 1)
        if $uowner->newpost_minsecurity eq "friends"
        and $security eq "public";

    my $qsecurity = $dbh->quote($security);

    ### make sure user can't post with "custom/private security" on shared journals
    return fail($err,102)
        if ($ownerid != $posterid && # community post
            ($req->{'security'} eq "private" ||
            ($req->{'security'} eq "usemask" && $qallowmask != 1 )));

    # make sure this user isn't banned from posting here (if
    # this is a community journal)
    return fail($err,151) if
        LJ::is_banned($posterid, $ownerid);

    # don't allow backdated posts in communities
    return fail($err,152) if
        ($req->{'props'}->{"opt_backdated"} &&
         $uowner->{'journaltype'} ne "P");

    # do processing of embedded polls (doesn't add to database, just
    # does validity checking)
    my @polls = ();
    if (LJ::Poll->contains_new_poll(\$event))
    {
        return fail($err,301,"Your account type doesn't permit creating polls.")
            unless (LJ::get_cap($u, "makepoll")
                    || ($uowner->{'journaltype'} eq "C"
                        && LJ::get_cap($uowner, "makepoll")
                        && LJ::can_manage_other($u, $uowner)));

        my $error = "";
        @polls = LJ::Poll->new_from_html(\$event, \$error, {
            'journalid' => $ownerid,
            'posterid' => $posterid,
        });
        return fail($err,103,$error) if $error;
    }

    # convert RTE lj-embeds to normal lj-embeds
    $event = LJ::EmbedModule->transform_rte_post($event);

    # process module embedding
    LJ::EmbedModule->parse_module_embed($uowner, \$event);

    my $now = $dbcm->selectrow_array("SELECT UNIX_TIMESTAMP()");
    my $anum  = int(rand(256));

    # by default we record the true reverse time that the item was entered.
    # however, if backdate is on, we put the reverse time at the end of time
    # (which makes it equivalent to 1969, but get_recent_items will never load
    # it... where clause there is: < $LJ::EndOfTime).  but this way we can
    # have entries that don't show up on friends view, now that we don't have
    # the hints table to not insert into.
    my $rlogtime = $LJ::EndOfTime;
    unless ($req->{'props'}->{"opt_backdated"}) {
        $rlogtime -= $now;
    }

    my $dupsig = Digest::MD5::md5_hex(join('', map { $req->{$_} }
                                           qw(subject event usejournal security allowmask)));
    my $lock_key = "post-$ownerid";

    # release our duplicate lock
    my $release = sub {  $dbcm->do("SELECT RELEASE_LOCK(?)", undef, $lock_key); };

    # our own local version of fail that releases our lock first
    my $fail = sub { $release->(); return fail(@_); };

    my $res = {};
    my $res_done = 0;  # set true by getlock when post was duplicate, or error getting lock

    my $getlock = sub {
        my $r = $dbcm->selectrow_array("SELECT GET_LOCK(?, 2)", undef, $lock_key);
        unless ($r) {
            $res = undef;    # a failure case has an undef result
            fail($err,503);  # set error flag to "can't get lock";
            $res_done = 1;   # tell caller to bail out
            return;
        }
        my @parts = split(/:/, $u->{'dupsig_post'});
        if ($parts[0] eq $dupsig) {
            # duplicate!  let's make the client think this was just the
            # normal first response.
            $res->{'itemid'} = $parts[1];
            $res->{'anum'} = $parts[2];

            my $dup_entry = LJ::Entry->new($uowner, jitemid => $res->{'itemid'}, anum => $res->{'anum'});
            $res->{'url'} = $dup_entry->url;

            $res_done = 1;
            $release->();
        }
    };

    my $need_moderated = ( $uowner->{'moderated'} =~ /^[1A]$/ ) ? 1 : 0;
    if ( $uowner->{'moderated'} eq 'F' ) {
        ## Scan post for spam
        LJ::run_hook('spam_community_detector', $uowner, $req, \$need_moderated);
    }
    # if posting to a moderated community, store and bail out here
    if ($uowner->{'journaltype'} eq 'C' && $need_moderated && !$flags->{'nomod'}) {
        # don't moderate admins, moderators & pre-approved users
        my $dbh = LJ::get_db_writer();
        my $relcount = $dbh->selectrow_array("SELECT COUNT(*) FROM reluser ".
                                             "WHERE userid=$ownerid AND targetid=$posterid ".
                                             "AND type IN ('A','M','N')");
        unless ($relcount) {
            # moderation queue full?
            my $modcount = $dbcm->selectrow_array("SELECT COUNT(*) FROM modlog WHERE journalid=$ownerid");
            return fail($err, 407) if $modcount >= LJ::get_cap($uowner, "mod_queue");

            $modcount = $dbcm->selectrow_array("SELECT COUNT(*) FROM modlog ".
                                               "WHERE journalid=$ownerid AND posterid=$posterid");
            return fail($err, 408) if $modcount >= LJ::get_cap($uowner, "mod_queue_per_poster");

            $req->{'_moderate'}->{'authcode'} = LJ::make_auth_code(15);

            # create tag <lj-embed> from HTML-tag <embed>
            LJ::EmbedModule->parse_module_embed($uowner, \$req->{event});

            my $fr = $dbcm->quote(Storable::freeze($req));
            return fail($err, 409) if length($fr) > 200_000;

            # store
            my $modid = LJ::alloc_user_counter($uowner, "M");
            return fail($err, 501) unless $modid;

            $uowner->do("INSERT INTO modlog (journalid, modid, posterid, subject, logtime) ".
                        "VALUES ($ownerid, $modid, $posterid, ?, NOW())", undef,
                        LJ::text_trim($req->{'subject'}, 30, 0));
            return fail($err, 501) if $uowner->err;

            $uowner->do("INSERT INTO modblob (journalid, modid, request_stor) ".
                        "VALUES ($ownerid, $modid, $fr)");
            if ($uowner->err) {
                $uowner->do("DELETE FROM modlog WHERE journalid=$ownerid AND modid=$modid");
                return fail($err, 501);
            }

            # alert moderator(s)
            my $mods = LJ::load_rel_user($dbh, $ownerid, 'M') || [];
            if (@$mods) {
                # load up all these mods and figure out if they want email or not
                my $modlist = LJ::load_userids(@$mods);

                my @emails;
                my $ct;
                foreach my $mod (values %$modlist) {
                    last if $ct > 20;  # don't send more than 20 emails.

                    next unless $mod->is_visible;

                    LJ::load_user_props($mod, 'opt_nomodemail');
                    next if $mod->{opt_nomodemail};
                    next if $mod->{status} ne "A";

                    push @emails,
                        {
                            to          => $mod->email_raw,
                            browselang  => $mod->prop('browselang'),
                            charset     => $mod->mailencoding || 'utf-8',
                        };

                    ++$ct;
                }

                foreach my $to (@emails) {
                    # TODO: html/plain text.
                    my $body = LJ::Lang::get_text(
                        $to->{'browselang'},
                        'esn.moderated_submission.body', undef,
                        {
                            user        => $u->{'user'},
                            subject     => $req->{'subject'},
                            community   => $uowner->{'user'},
                            modid       => $modid,
                            siteroot    => $LJ::SITEROOT,
                            sitename    => $LJ::SITENAME,
                            moderateurl => "$LJ::SITEROOT/community/moderate.bml?authas=$uowner->{'user'}&modid=$modid",
                            viewurl     => "$LJ::SITEROOT/community/moderate.bml?authas=$uowner->{'user'}",
                        });

                    my $subject = LJ::Lang::get_text($to->{'browselang'},'esn.moderated_submission.subject');

                    LJ::send_mail({
                        'to'        => $to->{to},
                        'from'      => $LJ::ADMIN_EMAIL,
                        'charset'   => $to->{charset},
                        'subject'   => $subject,
                        'body'      => $body,
                    });
                }
            }

            my $msg = translate($u, "modpost", undef);
            return { 'message' => $msg };
        }
    } # /moderated comms

    # posting:

    $getlock->(); return $res if $res_done;

    # do rate-checking
    if ($u->{'journaltype'} ne "Y" && ! LJ::rate_log($u, "post", 1)) {
        return $fail->($err,405);
    }

    my $jitemid = LJ::alloc_user_counter($uowner, "L");
    return $fail->($err,501,"No itemid could be generated.") unless $jitemid;

    # bring in LJ::Entry with Class::Autouse
    LJ::Entry->can("dostuff");
    LJ::replycount_do($uowner, $jitemid, "init");

    # remove comments and logprops on new entry ... see comment by this sub for clarification
    LJ::Protocol::new_entry_cleanup_hack($u, $jitemid) if $LJ::NEW_ENTRY_CLEANUP_HACK;
    my $verb = $LJ::NEW_ENTRY_CLEANUP_HACK ? 'REPLACE' : 'INSERT';

    my $dberr;
    $uowner->log2_do(\$dberr, "INSERT INTO log2 (journalid, jitemid, posterid, eventtime, logtime, security, ".
                     "allowmask, replycount, year, month, day, revttime, rlogtime, anum) ".
                     "VALUES ($ownerid, $jitemid, $posterid, $qeventtime, FROM_UNIXTIME($now), $qsecurity, $qallowmask, ".
                     "0, $req->{'year'}, $req->{'mon'}, $req->{'day'}, $LJ::EndOfTime-".
                     "UNIX_TIMESTAMP($qeventtime), $rlogtime, $anum)");
    return $fail->($err,501,$dberr) if $dberr;

    LJ::MemCache::incr([$ownerid, "log2ct:$ownerid"]);
    LJ::memcache_kill($ownerid, "dayct2");

    # set userprops.
    {
        my %set_userprop;

        # keep track of itemid/anum for later potential duplicates
        $set_userprop{"dupsig_post"} = "$dupsig:$jitemid:$anum";

        # record the eventtime of the last update (for own journals only)
        $set_userprop{"newesteventtime"} = $eventtime
            if $posterid == $ownerid and not $req->{'props'}->{'opt_backdated'} and not $time_was_faked;

        LJ::set_userprop($u, \%set_userprop);
    }

    # end duplicate locking section
    $release->();

    my $ditemid = $jitemid * 256 + $anum;

    ### finish embedding stuff now that we have the itemid
    {
        ### this should NOT return an error, and we're mildly fucked by now
        ### if it does (would have to delete the log row up there), so we're
        ### not going to check it for now.

        my $error = "";
        foreach my $poll (@polls) {
            $poll->save_to_db(
                              journalid => $ownerid,
                              posterid  => $posterid,
                              ditemid   => $ditemid,
                              error     => \$error,
                              );

            my $pollid = $poll->pollid;

            $event =~ s/<lj-poll-placeholder>/<lj-poll-$pollid>/;
        }
    }
    #### /embedding

    ### extract links for meme tracking
    unless ($req->{'security'} eq "usemask" ||
            $req->{'security'} eq "private")
    {
        foreach my $url (LJ::get_urls($event)) {
            LJ::record_meme($url, $posterid, $ditemid, $ownerid);
        }
    }

    # record journal's disk usage
    my $bytes = length($event) + length($req->{'subject'});
    $uowner->dudata_set('L', $jitemid, $bytes);

    $uowner->do("$verb INTO logtext2 (journalid, jitemid, subject, event) ".
                "VALUES ($ownerid, $jitemid, ?, ?)", undef, $req->{'subject'},
                LJ::text_compress($event));
    if ($uowner->err) {
        my $msg = $uowner->errstr;
        LJ::delete_entry($uowner, $jitemid);   # roll-back
        return fail($err,501,"logtext:$msg");
    }
    LJ::MemCache::set([$ownerid,"logtext:$clusterid:$ownerid:$jitemid"],
                      [ $req->{'subject'}, $event ]);

    # keep track of custom security stuff in other table.
    if ($uselogsec) {
        $uowner->do("INSERT INTO logsec2 (journalid, jitemid, allowmask) ".
                    "VALUES ($ownerid, $jitemid, $qallowmask)");
        if ($uowner->err) {
            my $msg = $uowner->errstr;
            LJ::delete_entry($uowner, $jitemid);   # roll-back
            return fail($err,501,"logsec2:$msg");
        }
    }

    # Entry tags
    if ($req->{props} && defined $req->{props}->{taglist}) {
        # slightly misnamed, the taglist is/was normally a string, but now can also be an arrayref.
        my $taginput = $req->{props}->{taglist};

        my $logtag_opts = {
            remote => $u,
        };

        if (ref $taginput eq 'ARRAY') {
            $logtag_opts->{set} = [@$taginput];
            $req->{props}->{taglist} = join(", ", @$taginput);
        } else {
            $logtag_opts->{set_string} = $taginput;
        }

        my $rv = LJ::Tags::update_logtags($uowner, $jitemid, $logtag_opts);
    }

    ## copyright
    if (LJ::is_enabled('default_copyright', $u)) {
        $req->{'props'}->{'copyright'} = $u->prop('default_copyright')
            unless defined $req->{'props'}->{'copyright'};
        $req->{'props'}->{'copyright'} = 'P' # second try
            unless defined $req->{'props'}->{'copyright'};
    } else {
        delete $req->{'props'}->{'copyright'};
    }

    # meta-data
    if (%{$req->{'props'}}) {
        my $propset = {};
        foreach my $pname (keys %{$req->{'props'}}) {
            next unless $req->{'props'}->{$pname};
            next if $pname eq "revnum" || $pname eq "revtime";
            my $p = LJ::get_prop("log", $pname);
            next unless $p;
            next unless $req->{'props'}->{$pname};
            $propset->{$pname} = $req->{'props'}->{$pname};
        }
        my %logprops;
        LJ::set_logprop($uowner, $jitemid, $propset, \%logprops) if %$propset;

        # if set_logprop modified props above, we can set the memcache key
        # to be the hashref of modified props, since this is a new post
        LJ::MemCache::set([$uowner->{'userid'}, "logprop:$uowner->{'userid'}:$jitemid"],
                          \%logprops) if %logprops;
    }

    $dbh->do("UPDATE userusage SET timeupdate=NOW(), lastitemid=$jitemid ".
             "WHERE userid=$ownerid") unless $flags->{'notimeupdate'};
    LJ::MemCache::set([$ownerid, "tu:$ownerid"], pack("N", time()), 30*60);

    # argh, this is all too ugly.  need to unify more postpost stuff into async
    $u->invalidate_directory_record;

    # note this post in recentactions table
    LJ::note_recent_action($uowner, 'post');

    # if the post was public, and the user has not opted out, try to insert into the random table;
    # note we do INSERT INGORE since there will be lots of people posting every second, and that's
    # the granularity we use
    if ($security eq 'public' && LJ::u_equals($u, $uowner) && ! $u->prop('latest_optout')) {
        $u->do("INSERT IGNORE INTO random_user_set (posttime, userid) VALUES (UNIX_TIMESTAMP(), ?)",
               undef, $u->{userid});
    }

    my @jobs;  # jobs to add into TheSchwartz

    # notify weblogs.com of post if necessary
    if (!$LJ::DISABLED{'weblogs_com'} && $u->{'opt_weblogscom'} && LJ::get_cap($u, "weblogscom") &&
        $security eq "public" && !$req->{'props'}->{'opt_backdated'}) {
        push @jobs, TheSchwartz::Job->new_from_array("LJ::Worker::Ping::WeblogsCom", {
            'user' => $u->{'user'},
            'title' => $u->{'journaltitle'} || $u->{'name'},
            'url' => LJ::journal_base($u) . "/",
        });
      }

    my $entry = LJ::Entry->new($uowner, jitemid => $jitemid, anum => $anum);

    # run local site-specific actions
    LJ::run_hooks("postpost", {
        'itemid'    => $jitemid,
        'anum'      => $anum,
        'journal'   => $uowner,
        'poster'    => $u,
        'event'     => $event,
        'eventtime' => $eventtime,
        'subject'   => $req->{'subject'},
        'security'  => $security,
        'allowmask' => $qallowmask,
        'props'     => $req->{'props'},
        'entry'     => $entry,
        'jobs'      => \@jobs,  # for hooks to push jobs onto
        'req'       => $req,
        'res'       => $res,
    });

    # cluster tracking
    LJ::mark_user_active($u, 'post');
    LJ::mark_user_active($uowner, 'post') unless LJ::u_equals($u, $uowner);

    $res->{'itemid'} = $jitemid;  # by request of mart
    $res->{'anum'} = $anum;
    $res->{'url'} = $entry->url;

    push @jobs, LJ::Event::JournalNewEntry->new($entry)->fire_job;
    push @jobs, LJ::Event::UserNewEntry->new($entry)->fire_job if (!$LJ::DISABLED{'esn-userevents'} || $LJ::_T_FIRE_USERNEWENTRY);
    push @jobs, LJ::EventLogRecord::NewEntry->new($entry)->fire_job;

    # PubSubHubbub Support
    LJ::Feed::generate_hubbub_jobs($uowner, \@jobs) unless $uowner->is_syndicated;

    my $sclient = LJ::theschwartz();
    if ($sclient && @jobs) {
        my @handles = $sclient->insert_jobs(@jobs);
        # TODO: error on failure?  depends on the job I suppose?  property of the job?
    }

    return $res;
}

sub editevent
{
    my ($req, $err, $flags) = @_;
    un_utf8_request($req);

    return undef unless authenticate($req, $err, $flags);

    my $spam = 0;
    return undef unless LJ::run_hook('spam_detector', $req, \$spam);
    return fail($err,320) if $spam;

    # we check later that user owns entry they're modifying, so all
    # we care about for check_altusage is that the target journal
    # exists, and we want it to setup some data in $flags.
    $flags->{'ignorecanuse'} = 1;
    return undef unless check_altusage($req, $err, $flags);

    my $u = $flags->{'u'};
    my $ownerid = $flags->{'ownerid'};
    my $uowner = $flags->{'u_owner'} || $u;
    # Make sure we have a user object here
    $uowner = LJ::want_user($uowner) unless LJ::isu($uowner);
    my $clusterid = $uowner->{'clusterid'};
    my $posterid = $u->{'userid'};
    my $qallowmask = $req->{'allowmask'}+0;
    my $sth;

    my $itemid = $req->{'itemid'}+0;

    # underage users can't do this
    return fail($err,310) if $u->underage;

    # check the journal's read-only bit
    return fail($err,306) if LJ::get_cap($uowner, "readonly");

    # can't edit in deleted/suspended community
    return fail($err,307) unless $uowner->{'statusvis'} eq "V" || $uowner->is_readonly;

    my $dbcm = LJ::get_cluster_master($uowner);
    return fail($err,306) unless $dbcm;

    # can't specify both a custom security and 'friends-only'
    return fail($err, 203, "Invalid friends group security set.")
        if $qallowmask > 1 && $qallowmask % 2;

    ### make sure user can't change a post to "custom/private security" on shared journals
    return fail($err,102)
        if ($ownerid != $posterid && # community post
            ($req->{'security'} eq "private" ||
            ($req->{'security'} eq "usemask" && $qallowmask != 1 )));

    # make sure the new entry's under the char limit
    # NOTE: as in postevent, this requires $req->{event} to be binary data
    # but we've already removed the utf-8 flag in the XML-RPC path, and it
    # never gets set in the "flat" protocol path
    return fail($err,409) if length($req->{event}) >= LJ::BMAX_EVENT;

    # fetch the old entry from master database so we know what we
    # really have to update later.  usually people just edit one part,
    # not every field in every table.  reads are quicker than writes,
    # so this is worth it.
    my $oldevent = $dbcm->selectrow_hashref
        ("SELECT journalid AS 'ownerid', posterid, eventtime, logtime, ".
         "compressed, security, allowmask, year, month, day, ".
         "rlogtime, anum FROM log2 WHERE journalid=$ownerid AND jitemid=$itemid");

    ($oldevent->{subject}, $oldevent->{event}) = $dbcm->selectrow_array
        ("SELECT subject, event FROM logtext2 ".
         "WHERE journalid=$ownerid AND jitemid=$itemid");

    LJ::text_uncompress(\$oldevent->{'event'});

    # use_old_content indicates the subject and entry are not changing
    if ($flags->{'use_old_content'}) {
        $req->{'event'} = $oldevent->{event};
        $req->{'subject'} = $oldevent->{subject};
    }

    # kill seconds in eventtime, since we don't use it, then we can use 'eq' and such
    $oldevent->{'eventtime'} =~ s/:00$//;

    ### make sure this user is allowed to edit this entry
    return fail($err,302)
        unless ($ownerid == $oldevent->{'ownerid'});

    ### what can they do to somebody elses entry?  (in shared journal)
    ### can edit it if they own or maintain the journal, but not if the journal is read-only
    if ($posterid != $oldevent->{'posterid'} || $u->is_readonly || $uowner->is_readonly)
    {
        ## deleting.
        return fail($err,304)
            if ($req->{'event'} !~ /\S/ && !
                ($ownerid == $u->{'userid'} ||
                 # community account can delete it (ick)

                 LJ::can_manage_other($posterid, $ownerid)
                 # if user is a community maintainer they can delete
                 # it too (good)
                 ));

        ## editing:
        if ($req->{'event'} =~ /\S/) {
            return fail($err,303) if $posterid != $oldevent->{'posterid'};
            return fail($err,318) if $u->is_readonly;
            return fail($err,319) if $uowner->is_readonly;
        }
    }

    # simple logic for deleting an entry
    if (!$flags->{'use_old_content'} && $req->{'event'} !~ /\S/)
    {

        ## 23.11.2009. Next code added due to some hackers activities
        ## that use trojans to delete user's entries in theirs journals.
        if ($LJ::DELETING_ENTRIES_IS_DISABLED
            && $u->is_person and $u->userid eq $oldevent->{ownerid}
        ){
            my $qsecurity = $uowner->quote('private');
            my $dberr;
            $uowner->log2_do(\$dberr, "UPDATE log2 SET security=$qsecurity " .
                                       "WHERE journalid=$ownerid AND jitemid=$itemid");
            return fail($err,501,$dberr) if $dberr;
            return fail($err, 321);
        }

        # if their newesteventtime prop equals the time of the one they're deleting
        # then delete their newesteventtime.
        if ($u->{'userid'} == $uowner->{'userid'}) {
            LJ::load_user_props($u, { use_master => 1 }, "newesteventtime");
            if ($u->{'newesteventtime'} eq $oldevent->{'eventtime'}) {
                LJ::set_userprop($u, "newesteventtime", undef);
            }
        }

        # log this event, unless noauth is on, which means it is being done internally and we should
        # rely on them to log why they're deleting the entry if they need to.  that way we don't have
        # double entries, and we have as much information available as possible at the location the
        # delete is initiated.
        $uowner->log_event('delete_entry', {
                remote => $u,
                actiontarget => ($req->{itemid} * 256 + $oldevent->{anum}),
                method => 'protocol',
            })
            unless $flags->{noauth};

        # We must use property 'dupsig_post' in author of entry to be deleted, not in
        # remote user or journal owner!
        my $item = LJ::get_log2_row($uowner, $req->{'itemid'});
        my $poster = $item ? LJ::want_user($item->{'posterid'}) : '';

        LJ::delete_entry($uowner, $req->{'itemid'}, 'quick', $oldevent->{'anum'});

        # clear their duplicate protection, so they can later repost
        # what they just deleted.  (or something... probably rare.)
        LJ::set_userprop($poster, "dupsig_post", undef) if $poster;

        my $res = { 'itemid' => $itemid,
                    'anum' => $oldevent->{'anum'} };
        return $res;
    }

    # now make sure the new entry text isn't $CannotBeShown
    return fail($err, 210)
        if $req->{event} eq $CannotBeShown;

    # don't allow backdated posts in communities
    return fail($err,152) if
        ($req->{'props'}->{"opt_backdated"} &&
         $uowner->{'journaltype'} ne "P");

    # make year/mon/day/hour/min optional in an edit event,
    # and just inherit their old values
    {
        $oldevent->{'eventtime'} =~ /^(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d)/;
        $req->{'year'} = $1 unless defined $req->{'year'};
        $req->{'mon'} = $2+0 unless defined $req->{'mon'};
        $req->{'day'} = $3+0 unless defined $req->{'day'};
        $req->{'hour'} = $4+0 unless defined $req->{'hour'};
        $req->{'min'} = $5+0 unless defined $req->{'min'};
    }

    # updating an entry:
    return undef
        unless common_event_validation($req, $err, $flags);

    ### load existing meta-data
    my %curprops;
    LJ::load_log_props2($dbcm, $ownerid, [ $itemid ], \%curprops);

    ## handle meta-data (properties)
    my %props_byname = ();
    foreach my $key (keys %{$req->{'props'}}) {
        ## changing to something else?
        if ($curprops{$itemid}->{$key} ne $req->{'props'}->{$key}) {
            $props_byname{$key} = $req->{'props'}->{$key};
        }
    }

    my $event = $req->{'event'};
    my $owneru = LJ::load_userid($ownerid);
    $event = LJ::EmbedModule->transform_rte_post($event);
    LJ::EmbedModule->parse_module_embed($owneru, \$event);

    my $bytes = length($event) + length($req->{'subject'});

    my $eventtime = sprintf("%04d-%02d-%02d %02d:%02d",
                            map { $req->{$_} } qw(year mon day hour min));
    my $qeventtime = $dbcm->quote($eventtime);

    # preserve old security by default, use user supplied if it's understood
    my $security = $oldevent->{security};
    $security = $req->{security}
        if $req->{security} &&
           $req->{security} =~ /^(?:public|private|usemask)$/;

    my $do_tags = $req->{props} && defined $req->{props}->{taglist};
    if ($oldevent->{security} ne $security || $qallowmask != $oldevent->{allowmask}) {
        # FIXME: this is a hopefully temporary hack which deletes tags from the entry
        # when the security has changed.  the real fix is to make update_logtags aware
        # of security changes so it can update logkwsum appropriately.

        unless ($do_tags) {
            # we need to fix security on this entry's tags, but the user didn't give us a tag list
            # to work with, so we have to go get the tags on the entry, and construct a tag list,
            # in order to pass to update_logtags down at the bottom of this whole update
            my $tags = LJ::Tags::get_logtags($uowner, $itemid);
            $tags = $tags->{$itemid};
            $req->{props}->{taglist} = join(',', sort values %{$tags || {}});
            $do_tags = 1; # bleh, force the update later
        }

        LJ::Tags::delete_logtags($uowner, $itemid);
    }

    my $qyear = $req->{'year'}+0;
    my $qmonth = $req->{'mon'}+0;
    my $qday = $req->{'day'}+0;

    if ($eventtime ne $oldevent->{'eventtime'} ||
        $security ne $oldevent->{'security'} ||
        (!$curprops{$itemid}->{opt_backdated} && $req->{props}{opt_backdated}) ||
        $qallowmask != $oldevent->{'allowmask'})
    {
        # are they changing their most recent post?
        LJ::load_user_props($u, "newesteventtime");
        if ($u->{userid} == $uowner->{userid} &&
            $u->{newesteventtime} eq $oldevent->{eventtime}) {
            # did they change the time?
            if ($eventtime ne $oldevent->{eventtime}) {
                # the newesteventtime is this event's new time.
                LJ::set_userprop($u, "newesteventtime", $eventtime);
            } elsif (!$curprops{$itemid}->{opt_backdated} && $req->{props}{opt_backdated}) {
                # otherwise, if they set the backdated flag,
                # then we no longer know the newesteventtime.
                LJ::set_userprop($u, "newesteventtime", undef);
            }
        }

        my $qsecurity = $uowner->quote($security);
        my $dberr;
        $uowner->log2_do(\$dberr, "UPDATE log2 SET eventtime=$qeventtime, revttime=$LJ::EndOfTime-".
                         "UNIX_TIMESTAMP($qeventtime), year=$qyear, month=$qmonth, day=$qday, ".
                         "security=$qsecurity, allowmask=$qallowmask WHERE journalid=$ownerid ".
                         "AND jitemid=$itemid");
        return fail($err,501,$dberr) if $dberr;

        # update memcached
        my $sec = $qallowmask;
        $sec = 0 if $security eq 'private';
        $sec = 2**31 if $security eq 'public';

        my $row = pack("NNNNN", $oldevent->{'posterid'},
                       LJ::mysqldate_to_time($eventtime, 1),
                       LJ::mysqldate_to_time($oldevent->{'logtime'}, 1),
                       $sec,
                       $itemid*256 + $oldevent->{'anum'});

        LJ::MemCache::set([$ownerid, "log2:$ownerid:$itemid"], $row);

    }

    if ($security ne $oldevent->{'security'} ||
        $qallowmask != $oldevent->{'allowmask'})
    {
        if ($security eq "public" || $security eq "private") {
            $uowner->do("DELETE FROM logsec2 WHERE journalid=$ownerid AND jitemid=$itemid");
        } else {
            $uowner->do("REPLACE INTO logsec2 (journalid, jitemid, allowmask) ".
                        "VALUES ($ownerid, $itemid, $qallowmask)");
        }
        return fail($err,501,$dbcm->errstr) if $uowner->err;
    }

    LJ::MemCache::set([$ownerid,"logtext:$clusterid:$ownerid:$itemid"],
                      [ $req->{'subject'}, $event ]);

    if (!$flags->{'use_old_content'} && (
        $event ne $oldevent->{'event'} ||
        $req->{'subject'} ne $oldevent->{'subject'}))
    {
        $uowner->do("UPDATE logtext2 SET subject=?, event=? ".
                    "WHERE journalid=$ownerid AND jitemid=$itemid", undef,
                    $req->{'subject'}, LJ::text_compress($event));
        return fail($err,501,$uowner->errstr) if $uowner->err;

        # update disk usage
        $uowner->dudata_set('L', $itemid, $bytes);
    }

    # up the revision number
    $req->{'props'}->{'revnum'} = ($curprops{$itemid}->{'revnum'} || 0) + 1;
    $req->{'props'}->{'revtime'} = time();

    # handle tags if they're defined
    if ($do_tags) {
        my $tagerr = "";
        my $rv = LJ::Tags::update_logtags($uowner, $itemid, {
                set_string => $req->{props}->{taglist},
                remote => $u,
                err_ref => \$tagerr,
            });
    }

    if (LJ::is_enabled('default_copyright', $u)) {
        unless (defined $req->{'props'}->{'copyright'}) { # try 1: previous value
            $req->{'props'}->{'copyright'} = $curprops{$itemid}->{'copyright'};
        }
        unless (defined $req->{'props'}->{'copyright'}) { # try 2: global setting
            $req->{'props'}->{'copyright'} = $uowner->prop('default_copyright');
        }
        unless (defined $req->{'props'}->{'copyright'}) { # try 3: allow
            $req->{'props'}->{'copyright'} = 'P';
        }
    } else { # disabled feature
        delete $req->{'props'}->{'copyright'};
    }

    # handle the props
    {
        my $propset = {};
        foreach my $pname (keys %{$req->{'props'}}) {
            my $p = LJ::get_prop("log", $pname);
            next unless $p;
            $propset->{$pname} = $req->{'props'}->{$pname};
        }
        LJ::set_logprop($uowner, $itemid, $propset);

        if ($req->{'props'}->{'copyright'} ne $curprops{$itemid}->{'copyright'}) {
            LJ::Entry->new($ownerid, jitemid => $itemid)->put_logprop_in_history('copyright', $curprops{$itemid}->{'copyright'},
                                                                                  $req->{'props'}->{'copyright'});
        }
    }

    # deal with backdated changes.  if the entry's rlogtime is
    # $EndOfTime, then it's backdated.  if they want that off, need to
    # reset rlogtime to real reverse log time.  also need to set
    # rlogtime to $EndOfTime if they're turning backdate on.
    if ($req->{'props'}->{'opt_backdated'} eq "1" &&
        $oldevent->{'rlogtime'} != $LJ::EndOfTime) {
        my $dberr;
        $uowner->log2_do(undef, "UPDATE log2 SET rlogtime=$LJ::EndOfTime WHERE ".
                         "journalid=$ownerid AND jitemid=$itemid");
        return fail($err,501,$dberr) if $dberr;
    }
    if ($req->{'props'}->{'opt_backdated'} eq "0" &&
        $oldevent->{'rlogtime'} == $LJ::EndOfTime) {
        my $dberr;
        $uowner->log2_do(\$dberr, "UPDATE log2 SET rlogtime=$LJ::EndOfTime-UNIX_TIMESTAMP(logtime) ".
                         "WHERE journalid=$ownerid AND jitemid=$itemid");
        return fail($err,501,$dberr) if $dberr;
    }
    return fail($err,501,$dbcm->errstr) if $dbcm->err;

    LJ::memcache_kill($ownerid, "dayct2");

    my $res = { 'itemid' => $itemid };
    if (defined $oldevent->{'anum'}) {
        $res->{'anum'} = $oldevent->{'anum'};
        $res->{'url'} = LJ::item_link($uowner, $itemid, $oldevent->{'anum'});
    }

    my $entry = LJ::Entry->new($ownerid, jitemid => $itemid);
    LJ::EventLogRecord::EditEntry->new($entry)->fire;
    my @jobs; # jobs to insert into TheSchwartz
    LJ::run_hooks("editpost", $entry, \@jobs);

    # PubSubHubbub Support
    LJ::Feed::generate_hubbub_jobs($uowner, \@jobs) unless $uowner->is_syndicated;

    my $sclient = LJ::theschwartz();
    if ($sclient && @jobs) {
        my @handles = $sclient->insert_jobs(@jobs);
        # TODO: error on failure?  depends on the job I suppose?  property of the job?
    }

    return $res;
}

sub getevents
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    return undef unless check_altusage($req, $err, $flags);

    my $u = $flags->{'u'};
    my $uowner = $flags->{'u_owner'} || $u;

    ### shared-journal support
    my $posterid = $u->{'userid'};
    my $ownerid = $flags->{'ownerid'};

    my $dbr = LJ::get_db_reader();
    my $sth;

    my $dbcr =  LJ::get_cluster_reader($uowner);
    return fail($err,502) unless $dbcr && $dbr;

    # can't pull events from deleted/suspended journal
    return fail($err,307) unless $uowner->{'statusvis'} eq "V" || $uowner->is_readonly;

    my $reject_code = $LJ::DISABLE_PROTOCOL{getevents};
    if (ref $reject_code eq "CODE") {
        my $r = eval { Apache->request };
        my $errmsg = $reject_code->($req, $flags, $r);
        if ($errmsg) { return fail($err, "311", $errmsg); }
    }

    # if this is on, we sort things different (logtime vs. posttime)
    # to avoid timezone issues
    my $is_community = ($uowner->{'journaltype'} eq "C" ||
                        $uowner->{'journaltype'} eq "S");

    # in some cases we'll use the master, to ensure there's no
    # replication delay.  useful cases: getting one item, use master
    # since user might have just made a typo and realizes it as they
    # post, or wants to append something they forgot, etc, etc.  in
    # other cases, slave is pretty sure to have it.
    my $use_master = 0;

    # the benefit of this mode over actually doing 'lastn/1' is
    # the $use_master usage.
    if ($req->{'selecttype'} eq "one" && $req->{'itemid'} eq "-1") {
        $req->{'selecttype'} = "lastn";
        $req->{'howmany'} = 1;
        undef $req->{'itemid'};
        $use_master = 1;  # see note above.
    }

    # just synonym
    if ($req->{'itemshow'}){
        $req->{'selecttype'} = 'lastn' unless $req->{'selecttype'};
        $req->{'howmany'} = $req->{'itemshow'};
    }
    my $skip = $req->{'skip'} + 0;
    if ($skip > 500) { $skip = 500; }
    
    # build the query to get log rows.  each selecttype branch is
    # responsible for either populating the following 3 variables
    # OR just populating $sql
    my ($orderby, $where, $limit, $offset);
    my $sql;
    if ($req->{'selecttype'} eq "day")
    {
        return fail($err,203)
            unless ($req->{'year'} =~ /^\d\d\d\d$/ &&
                    $req->{'month'} =~ /^\d\d?$/ &&
                    $req->{'day'} =~ /^\d\d?$/ &&
                    $req->{'month'} >= 1 && $req->{'month'} <= 12 &&
                    $req->{'day'} >= 1 && $req->{'day'} <= 31);

        my $qyear = $dbr->quote($req->{'year'});
        my $qmonth = $dbr->quote($req->{'month'});
        my $qday = $dbr->quote($req->{'day'});
        $where = "AND year=$qyear AND month=$qmonth AND day=$qday";
        $limit = "LIMIT 200";  # FIXME: unhardcode this constant (also in ljviews.pl)

        # see note above about why the sort order is different
        $orderby = $is_community ? "ORDER BY logtime" : "ORDER BY eventtime";
    }
    elsif ($req->{'selecttype'} eq "lastn")
    {
        my $howmany = $req->{'howmany'} || 20;
        if ($howmany > 50) { $howmany = 50; }
        $howmany = $howmany + 0;
        $limit = "LIMIT $howmany";

        $offset = "OFFSET $skip";

        # okay, follow me here... see how we add the revttime predicate
        # even if no beforedate key is present?  you're probably saying,
        # that's retarded -- you're saying: "revttime > 0", that's like
        # saying, "if entry occurred at all."  yes yes, but that hints
        # mysql's braindead optimizer to use the right index.
        my $rtime_after = 0;
        my $rtime_what = $is_community ? "rlogtime" : "revttime";
        if ($req->{'beforedate'}) {
            return fail($err,203,"Invalid beforedate format.")
                unless ($req->{'beforedate'} =~
                        /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/);
            my $qd = $dbr->quote($req->{'beforedate'});
            $rtime_after = "$LJ::EndOfTime-UNIX_TIMESTAMP($qd)";
        }
        $where .= "AND $rtime_what > $rtime_after ";
        $orderby = "ORDER BY $rtime_what";
    }
    elsif ($req->{'selecttype'} eq "one")
    {
        my $id = $req->{'itemid'} + 0;
        $where = "AND jitemid=$id";
    }
    elsif ($req->{'selecttype'} eq "syncitems")
    {
        return fail($err,506) if $LJ::DISABLED{'syncitems'};
        my $date = $req->{'lastsync'} || "0000-00-00 00:00:00";
        return fail($err,203,"Invalid syncitems date format")
            unless ($date =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d/);

        my $now = time();
        # broken client loop prevention
        if ($req->{'lastsync'}) {
            my $pname = "rl_syncitems_getevents_loop";
            LJ::load_user_props($u, $pname);
            # format is:  time/date/time/date/time/date/... so split
            # it into a hash, then delete pairs that are older than an hour
            my %reqs = split(m!/!, $u->{$pname});
            foreach (grep { $_ < $now - 60*60 } keys %reqs) { delete $reqs{$_}; }
            my $count = grep { $_ eq $date } values %reqs;
            $reqs{$now} = $date;
            if ($count >= 2) {
                # 2 prior, plus this one = 3 repeated requests for same synctime.
                # their client is busted.  (doesn't understand syncitems semantics)
                return fail($err,406);
            }
            LJ::set_userprop($u, $pname,
                             join('/', map { $_, $reqs{$_} }
                                  sort { $b <=> $a } keys %reqs));
        }

        my %item;
        $sth = $dbcr->prepare("SELECT jitemid, logtime FROM log2 WHERE ".
                              "journalid=? and logtime > ?");
        $sth->execute($ownerid, $date);
        while (my ($id, $dt) = $sth->fetchrow_array) {
            $item{$id} = $dt;
        }

        my $p_revtime = LJ::get_prop("log", "revtime");
        $sth = $dbcr->prepare("SELECT jitemid, FROM_UNIXTIME(value) ".
                              "FROM logprop2 WHERE journalid=? ".
                              "AND propid=$p_revtime->{'id'} ".
                              "AND value+0 > UNIX_TIMESTAMP(?)");
        $sth->execute($ownerid, $date);
        while (my ($id, $dt) = $sth->fetchrow_array) {
            $item{$id} = $dt;
        }

        my $limit = 100;
        my @ids = sort { $item{$a} cmp $item{$b} } keys %item;
        if (@ids > $limit) { @ids = @ids[0..$limit-1]; }

        my $in = join(',', @ids) || "0";
        $where = "AND jitemid IN ($in)";
    }
    elsif ($req->{'selecttype'} eq "multiple")
    {
        my @ids;
        foreach my $num (split(/\s*,\s*/, $req->{'itemids'})) {
            return fail($err,203,"Non-numeric itemid") unless $num =~ /^\d+$/;
            push @ids, $num;
        }
        my $limit = 100;
        return fail($err,209,"Can't retrieve more than $limit entries at once") if @ids > $limit;
        my $in = join(',', @ids);
        $where = "AND jitemid IN ($in)";
    }
    elsif ($req->{'selecttype'} eq 'before')
    {
        my $before = $req->{'before'};
        my $itemshow = $req->{'howmany'};
        my $itemselect = $itemshow + $skip;

        my %item;
        $sth = $dbcr->prepare("SELECT jitemid, logtime FROM log2 WHERE ".
                              "journalid=? and logtime < ? LIMIT $itemselect");
        $sth->execute($ownerid, $before);
        while (my ($id, $dt) = $sth->fetchrow_array) {
            $item{$id} = $dt;
            
        }

        my $p_revtime = LJ::get_prop("log", "revtime");

        $sth = $dbcr->prepare("SELECT jitemid, FROM_UNIXTIME(value) ".
                              "FROM logprop2 WHERE journalid=? ".
                              "AND propid=$p_revtime->{'id'} ".
                              "AND value+0 < ? LIMIT $itemselect");
        $sth->execute($ownerid, $before);
        while (my ($id, $dt) = $sth->fetchrow_array) {
            $item{$id} = $dt;
        }

        my @ids = sort { $item{$a} cmp $item{$b} } keys %item;        
        if (@ids > $skip){
            @ids = @ids[$skip..(@ids-1)];
            @ids = @ids[0..$itemshow-1] if @ids > $itemshow;
        }else{
            @ids = ();
        }

        my $in = join(',', @ids) || "0";
        $where = "AND jitemid IN ($in)";
    }
    else
    {
        return fail($err,200,"Invalid selecttype.");
    }

    my $secmask = 0;
    if ($u && ($u->{'journaltype'} eq "P" || $u->{'journaltype'} eq "I") && $posterid != $ownerid) {
        $secmask = LJ::get_groupmask($ownerid, $posterid);
    }

    # decide what level of security the remote user can see
    # 'getevents' used in small count of places and we will not pass 'viewall' through their call chain
    my $secwhere = "";
    if ($posterid == $ownerid) {
        # no extra where restrictions... user can see all their own stuff
    } elsif ($secmask) {
        # can see public or things with them in the mask
        $secwhere = "AND (security='public' OR (security='usemask' AND allowmask & $secmask != 0) OR posterid=$posterid)";
    } else {
        # not a friend?  only see public.
        $secwhere = "AND (security='public' OR posterid=$posterid)";
    }

    # common SQL template:
    unless ($sql) {
        $sql = "SELECT jitemid, eventtime, security, allowmask, anum, posterid, replycount, UNIX_TIMESTAMP(eventtime) ".
            "FROM log2 WHERE journalid=$ownerid $secwhere $where $orderby $limit $offset";
    }

    # whatever selecttype might have wanted us to use the master db.
    $dbcr = LJ::get_cluster_def_reader($uowner) if $use_master;

    return fail($err,502) unless $dbcr;

    ## load the log rows
    ($sth = $dbcr->prepare($sql))->execute;
    return fail($err,501,$dbcr->errstr) if $dbcr->err;

    my $count = 0;
    my @itemids = ();
    my $res = { skip => $skip };
    my $events = $res->{'events'} = [];
    my %evt_from_itemid;

    while (my ($itemid, $eventtime, $sec, $mask, $anum, $jposterid, $replycount, $event_timestamp) = $sth->fetchrow_array)
    {
        $count++;
        my $evt = {};
        $evt->{'itemid'} = $itemid;
        push @itemids, $itemid;

        $evt_from_itemid{$itemid} = $evt;

        $evt->{"eventtime"} = $eventtime;
        $evt->{event_timestamp} = $event_timestamp;
        if ($sec ne "public") {
            $evt->{'security'} = $sec;
            $evt->{'allowmask'} = $mask if $sec eq "usemask";
        }
        $evt->{'anum'} = $anum;
        $evt->{'poster'} = LJ::get_username($dbr, $jposterid) if $jposterid != $ownerid;
        $evt->{'url'} = LJ::item_link($uowner, $itemid, $anum);
        $evt->{'reply_count'} = $replycount;
        push @$events, $evt;
    }

    # load properties. Even if the caller doesn't want them, we need
    # them in Unicode installations to recognize older 8bit non-UF-8
    # entries.
    unless ($req->{'noprops'} && !$LJ::UNICODE)
    {
        ### do the properties now
        $count = 0;
        my %props = ();
        LJ::load_log_props2($dbcr, $ownerid, \@itemids, \%props);

        # load the tags for these entries, unless told not to
        unless ($req->{notags}) {
            # construct %idsbycluster for the multi call to get these tags
            my $tags = LJ::Tags::get_logtags($uowner, \@itemids);

            # add to props
            foreach my $itemid (@itemids) {
                next unless $tags->{$itemid};
                $props{$itemid}->{taglist} = join(', ', values %{$tags->{$itemid}});
            }
        }

        foreach my $itemid (keys %props) {
            # 'replycount' is a pseudo-prop, don't send it.
            # FIXME: this goes away after we restructure APIs and
            # replycounts cease being transferred in props
            delete $props{$itemid}->{'replycount'};

            my $evt = $evt_from_itemid{$itemid};
            $evt->{'props'} = {};
            foreach my $name (keys %{$props{$itemid}}) {
                my $value = $props{$itemid}->{$name};
                $value =~ s/\n/ /g;
                $evt->{'props'}->{$name} = $value;
            }
        }
    }

    ## load the text
    my $text = LJ::cond_no_cache($use_master, sub {
        return LJ::get_logtext2($uowner, @itemids);
    });

    foreach my $i (@itemids)
    {
        my $t = $text->{$i};
        my $evt = $evt_from_itemid{$i};

        # if they want subjects to be events, replace event
        # with subject when requested.
        if ($req->{'prefersubject'} && length($t->[0])) {
            $t->[1] = $t->[0];  # event = subject
            $t->[0] = undef;    # subject = undef
        }

        # now that we have the subject, the event and the props,
        # auto-translate them to UTF-8 if they're not in UTF-8.
        if ($LJ::UNICODE && $req->{'ver'} >= 1 &&
                $evt->{'props'}->{'unknown8bit'}) {
            my $error = 0;
            $t->[0] = LJ::text_convert($t->[0], $uowner, \$error);
            $t->[1] = LJ::text_convert($t->[1], $uowner, \$error);
            foreach (keys %{$evt->{'props'}}) {
                $evt->{'props'}->{$_} = LJ::text_convert($evt->{'props'}->{$_}, $uowner, \$error);
            }
            return fail($err,208,"Cannot display this post. Please see $LJ::SITEROOT/support/encodings.bml for more information.")
                if $error;
        }

        if ($LJ::UNICODE && $req->{'ver'} < 1 && !$evt->{'props'}->{'unknown8bit'}) {
            unless ( LJ::is_ascii($t->[0]) &&
                     LJ::is_ascii($t->[1]) &&
                     LJ::is_ascii(join(' ', values %{$evt->{'props'}}) )) {
                # we want to fail the client that wants to get this entry
                # but we make an exception for selecttype=day, in order to allow at least
                # viewing the daily summary

                if ($req->{'selecttype'} eq 'day') {
                    $t->[0] = $t->[1] = $CannotBeShown;
                } else {
                    return fail($err,207,"Cannot display/edit a Unicode post with a non-Unicode client. Please see $LJ::SITEROOT/support/encodings.bml for more information.");
                }
            }
        }

        if ($t->[0]) {
            $t->[0] =~ s/[\r\n]/ /g;
            $evt->{'subject'} = $t->[0];
        }

        $t->[1] = LJ::trim_widgets(
            length     => $req->{trim_widgets},
            img_length => $req->{widgets_img_length},
            text      => $t->[1],
            read_more => '<a href="' . $evt->{url} . '"> ...</a>',
        ) if $req->{trim_widgets};

        $t->[1] = LJ::convert_lj_tags_to_links(
            event => $t->[1],
            embed_url => $evt->{url},
        ) if $req->{parseljtags};


        # truncate
        if ($req->{'truncate'} >= 4) {
            my $original = $t->[1];
            if ($req->{'ver'} > 1) {
                $t->[1] = LJ::text_trim($t->[1], $req->{'truncate'} - 3, 0);
            } else {
                $t->[1] = LJ::text_trim($t->[1], 0, $req->{'truncate'} - 3);
            }
            # only append the elipsis if the text was actually truncated
            $t->[1] .= "..." if $t->[1] ne $original;
        }

        # line endings
        $t->[1] =~ s/\r//g;
        if ($req->{'lineendings'} eq "unix") {
            # do nothing.  native format.
        } elsif ($req->{'lineendings'} eq "mac") {
            $t->[1] =~ s/\n/\r/g;
        } elsif ($req->{'lineendings'} eq "space") {
            $t->[1] =~ s/\n/ /g;
        } elsif ($req->{'lineendings'} eq "dots") {
            $t->[1] =~ s/\n/ ... /g;
        } else { # "pc" -- default
            $t->[1] =~ s/\n/\r\n/g;
        }
        $evt->{'event'} = $t->[1];
    }

    # maybe we don't need the props after all
    if ($req->{'noprops'}) {
        foreach(@$events) { delete $_->{'props'}; }
    }

    return $res;
}

sub editfriends
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);

    my $u = $flags->{'u'};
    my $userid = $u->{'userid'};
    my $dbh = LJ::get_db_writer();
    my $sth;

    return fail($err,306) unless $dbh;

    # do not let locked people do this
    return fail($err, 308) if $u->{statusvis} eq 'L';

#
# Do not have values for $LJ::ADD_FRIEND_RATE_LIMIT
#
#    # check action frequency
#    unless ($flags->{no_rate_check}){
#        my $cond = ["ratecheck:add_friend:$userid",
#                    [ $LJ::ADD_FRIEND_RATE_LIMIT || [ 10, 600 ] ]
#                   ];
#        return fail($err, 411)
#            unless LJ::RateLimit->check($u, [ $cond ]);
#    }

    my $res = {};

    ## first, figure out who the current friends are to save us work later
    my %curfriend;
    my $friend_count = 0;
    my $friends_changed = 0;

    # TAG:FR:protocol:editfriends1
    $sth = $dbh->prepare("SELECT u.user FROM useridmap u, friends f ".
                         "WHERE u.userid=f.friendid AND f.userid=$userid");
    $sth->execute;
    while (my ($friend) = $sth->fetchrow_array) {
        $curfriend{$friend} = 1;
        $friend_count++;
    }
    $sth->finish;

    # perform the deletions
  DELETEFRIEND:
    foreach (@{$req->{'delete'}})
    {
        my $deluser = LJ::canonical_username($_);
        next DELETEFRIEND unless ($curfriend{$deluser});

        my $friendid = LJ::get_userid($deluser);
        # TAG:FR:protocol:editfriends2_del
        LJ::remove_friend($userid, $friendid);
        $friend_count--;
        $friends_changed = 1;
    }

    my $error_flag = 0;
    my $friends_added = 0;
    my $fail = sub {
        LJ::memcache_kill($userid, "friends");
        LJ::mark_dirty($userid, "friends");
        return fail($err, $_[0], $_[1]);
    };

    # only people, shared journals, and owned syn feeds can add friends
    return $fail->(104, "Journal type cannot add friends")
        unless ($u->{'journaltype'} eq 'P' ||
                $u->{'journaltype'} eq 'S' ||
                $u->{'journaltype'} eq 'I' ||
                ($u->{'journaltype'} eq "Y" && $u->password));

    # Don't let suspended users add friend
    return $fail->(305, "Suspended journals cannot add friends.")
        if ($u->is_suspended);

     my $sclient = LJ::theschwartz();

    # perform the adds
  ADDFRIEND:
    foreach my $fa (@{$req->{'add'}})
    {
        unless (ref $fa eq "HASH") {
            $fa = { 'username' => $fa };
        }

        my $aname = LJ::canonical_username($fa->{'username'});
        unless ($aname) {
            $error_flag = 1;
            next ADDFRIEND;
        }

        $friend_count++ unless $curfriend{$aname};

        my $err;
        return $fail->(104, "$err")
            unless $u->can_add_friends(\$err, { 'numfriends' => $friend_count, friend => $fa });

        my $fg = $fa->{'fgcolor'} || "#000000";
        my $bg = $fa->{'bgcolor'} || "#FFFFFF";
        if ($fg !~ /^\#[0-9A-F]{6,6}$/i || $bg !~ /^\#[0-9A-F]{6,6}$/i) {
            return $fail->(203, "Invalid color values");
        }

        my $row = LJ::load_user($aname);
        my $currently_is_friend = LJ::is_friend($u, $row);
        my $currently_is_banned = LJ::is_banned($u, $row);

        # XXX - on some errors we fail out, on others we continue and try adding
        # any other users in the request. also, error message for redirect should
        # point the user to the redirected username.
        if (! $row) {
            $error_flag = 1;
        } elsif ($row->{'journaltype'} eq "R") {
            return $fail->(154);
        } elsif ($row->{'statusvis'} ne "V") {
            $error_flag = 1;
        } else {
            $friends_added++;
            my $added = { 'username' => $aname,
                          'fullname' => $row->{'name'},
                      };
            if ($req->{'ver'} >= 1) {
                LJ::text_out(\$added->{'fullname'});
            }
            push @{$res->{'added'}}, $added;

            my $qfg = LJ::color_todb($fg);
            my $qbg = LJ::color_todb($bg);

            my $friendid = $row->{'userid'};

            my $gmask = $fa->{'groupmask'};
            if (! $gmask && $curfriend{$aname}) {
                # if no group mask sent, use the existing one if this is an existing friend
                # TAG:FR:protocol:editfriends3_getmask
                my $sth = $dbh->prepare("SELECT groupmask FROM friends ".
                                        "WHERE userid=$userid AND friendid=$friendid");
                $sth->execute;
                $gmask = $sth->fetchrow_array;
            }
            # force bit 0 on.
            $gmask |= 1;

            # TAG:FR:protocol:editfriends4_addeditfriend
            my $cnt = $dbh->do("REPLACE INTO friends (userid, friendid, fgcolor, bgcolor, groupmask) ".
                               "VALUES ($userid, $friendid, $qfg, $qbg, $gmask)");
            return $fail->(501,$dbh->errstr) if $dbh->err;

            if ($cnt == 1) {
                LJ::run_hooks('befriended', LJ::load_userid($userid), LJ::load_userid($friendid));
            }

            my $memkey = [$userid,"frgmask:$userid:$friendid"];
            LJ::MemCache::set($memkey, $gmask+0, time()+60*15);
            LJ::memcache_kill($friendid, 'friendofs');
            LJ::memcache_kill($friendid, 'friendofs2');

            if ($sclient && !$currently_is_friend && !$currently_is_banned) {
                my @jobs;
                push @jobs, LJ::Event::Befriended->new(LJ::load_userid($friendid), LJ::load_userid($userid))->fire_job
                    if !$LJ::DISABLED{esn};

                push @jobs, TheSchwartz::Job->new(
                                                  funcname => "LJ::Worker::FriendChange",
                                                  arg      => [$userid, 'add', $friendid],
                                                  ) unless $LJ::DISABLED{'friendchange-schwartz'};

                $sclient->insert_jobs(@jobs) if @jobs;
            }
            $friends_changed = 1;
        }
    }

    return $fail->(104) if $error_flag;

    # invalidate memcache of friends
    LJ::memcache_kill($userid, "friends");
    LJ::memcache_kill($userid, "friends2");
    LJ::mark_dirty($userid, "friends");

    LJ::run_hooks('friends_changed', LJ::load_userid($userid)) if $friends_changed;

    return $res;
}

sub editfriendgroups
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);

    my $u = $flags->{'u'};
    my $userid = $u->{'userid'};
    my ($db, $fgtable, $bmax, $cmax) = $u->{dversion} > 5 ?
                         ($u->writer, 'friendgroup2', LJ::BMAX_GRPNAME2, LJ::CMAX_GRPNAME2) :
                         (LJ::get_db_writer(), 'friendgroup', LJ::BMAX_GRPNAME, LJ::CMAX_GRPNAME);
    my $sth;

    return fail($err,306) unless $db;

    # do not let locked people do this
    return fail($err, 308) if $u->{statusvis} eq 'L';

    my $res = {};

    ## make sure tree is how we want it
    $req->{'groupmasks'} = {} unless
        (ref $req->{'groupmasks'} eq "HASH");
    $req->{'set'} = {} unless
        (ref $req->{'set'} eq "HASH");
    $req->{'delete'} = [] unless
        (ref $req->{'delete'} eq "ARRAY");

    # Keep track of what bits are already set, so we can know later
    # whether to INSERT or UPDATE.
    my %bitset;
    my $groups = LJ::get_friend_group($userid);
    foreach my $bit (keys %{$groups || {}}) {
        $bitset{$bit} = 1;
    }

    ## before we perform any DB operations, validate input text
    # (groups' names) for correctness so we can fail gracefully
    if ($LJ::UNICODE) {
        foreach my $bit (keys %{$req->{'set'}})
        {
            my $name = $req->{'set'}->{$bit}->{'name'};
            return fail($err,207,"non-ASCII names require a Unicode-capable client")
                if $req->{'ver'} < 1 and not LJ::is_ascii($name);
            return fail($err,208,"Invalid group names. Please see $LJ::SITEROOT/support/encodings.bml for more information.")
                unless LJ::text_in($name);
        }
    }

    ## figure out deletions we'll do later
    foreach my $bit (@{$req->{'delete'}})
    {
        $bit += 0;
        next unless ($bit >= 1 && $bit <= 30);
        $bitset{$bit} = 0;  # so later we replace into, not update.
    }

    ## do additions/modifications ('set' hash)
    my %added;
    foreach my $bit (keys %{$req->{'set'}})
    {
        $bit += 0;
        next unless ($bit >= 1 && $bit <= 30);
        my $sa = $req->{'set'}->{$bit};
        my $name = LJ::text_trim($sa->{'name'}, $bmax, $cmax);

        # can't end with a slash
        $name =~ s!/$!!;

        # setting it to name is like deleting it.
        unless ($name =~ /\S/) {
            push @{$req->{'delete'}}, $bit;
            next;
        }

        my $qname = $db->quote($name);
        my $qsort = defined $sa->{'sort'} ? ($sa->{'sort'}+0) : 50;
        my $qpublic = $db->quote(defined $sa->{'public'} ? ($sa->{'public'}+0) : 0);

        if ($bitset{$bit}) {
            # so update it
            my $sets;
            if (defined $sa->{'public'}) {
                $sets .= ", is_public=$qpublic";
            }
            $db->do("UPDATE $fgtable SET groupname=$qname, sortorder=$qsort ".
                    "$sets WHERE userid=$userid AND groupnum=$bit");
        } else {
            $db->do("REPLACE INTO $fgtable (userid, groupnum, ".
                    "groupname, sortorder, is_public) VALUES ".
                    "($userid, $bit, $qname, $qsort, $qpublic)");
        }
        $added{$bit} = 1;
    }


    ## do deletions ('delete' array)
    my $dbcm = LJ::get_cluster_master($u);

    # ignore bits that aren't integers or that are outside 1-30 range
    my @delete_bits = grep {$_ >= 1 and $_ <= 30} map {$_+0} @{$req->{'delete'}};
    my $delete_mask = 0;
    foreach my $bit (@delete_bits) {
        $delete_mask |= (1 << $bit)
    }

    # remove the bits for deleted groups from all friends groupmasks
    my $dbh = LJ::get_db_writer();
    if ($delete_mask) {
        # TAG:FR:protocol:editfriendgroups_removemasks
        $dbh->do("UPDATE friends".
                 "   SET groupmask = groupmask & ~$delete_mask".
                 " WHERE userid = $userid");
    }

    foreach my $bit (@delete_bits)
    {
        # remove all posts from allowing that group:
        my @posts_to_clean = ();
        $sth = $dbcm->prepare("SELECT jitemid FROM logsec2 WHERE journalid=$userid AND allowmask & (1 << $bit)");
        $sth->execute;
        while (my ($id) = $sth->fetchrow_array) { push @posts_to_clean, $id; }
        while (@posts_to_clean) {
            my @batch;
            if (scalar(@posts_to_clean) < 20) {
                @batch = @posts_to_clean;
                @posts_to_clean = ();
            } else {
                @batch = splice(@posts_to_clean, 0, 20);
            }

            my $in = join(",", @batch);
            $u->do("UPDATE log2 SET allowmask=allowmask & ~(1 << $bit) ".
                   "WHERE journalid=$userid AND jitemid IN ($in) AND security='usemask'");
            $u->do("UPDATE logsec2 SET allowmask=allowmask & ~(1 << $bit) ".
                   "WHERE journalid=$userid AND jitemid IN ($in)");

            foreach my $id (@batch) {
                LJ::MemCache::delete([$userid, "log2:$userid:$id"]);
            }
            LJ::MemCache::delete([$userid, "log2lt:$userid"]);
        }
        LJ::Tags::deleted_friend_group($u, $bit);
        LJ::run_hooks('delete_friend_group', $u, $bit);

        # remove the friend group, unless we just added it this transaction
        unless ($added{$bit}) {
            $db->do("DELETE FROM $fgtable WHERE ".
                    "userid=$userid AND groupnum=$bit");
        }
    }

    ## change friends' masks
    # TAG:FR:protocol:editfriendgroups_changemasks
    foreach my $friend (keys %{$req->{'groupmasks'}})
    {
        my $mask = int($req->{'groupmasks'}->{$friend}) | 1;
        my $friendid = LJ::get_userid($dbh, $friend);

        $dbh->do("UPDATE friends SET groupmask=$mask ".
                 "WHERE userid=$userid AND friendid=?",
                 undef, $friendid);
        LJ::MemCache::set([$userid, "frgmask:$userid:$friendid"], $mask);
    }

    # invalidate memcache of friends/groups
    LJ::memcache_kill($userid, "friends");
    LJ::memcache_kill($userid, "fgrp");
    LJ::mark_dirty($u, "friends");

    # return value for this is nothing.
    return {};
}

sub sessionexpire {
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    my $u = $flags->{u};

    # expunge one? or all?
    if ($req->{expireall}) {
        $u->kill_all_sessions;
        return {};
    }

    # just expire a list
    my $list = $req->{expire} || [];
    return {} unless @$list;
    return fail($err,502) unless $u->writer;
    $u->kill_sessions(@$list);
    return {};
}

sub sessiongenerate {
    # generate a session
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);

    # sanitize input
    $req->{expiration} = 'short' unless $req->{expiration} eq 'long';
    my $boundip;
    $boundip = LJ::get_remote_ip() if $req->{bindtoip};

    my $u = $flags->{u};
    my $sess_opts = {
        exptype => $req->{expiration},
        ipfixed => $boundip,
    };

    # do not let locked people do this
    return fail($err, 308) if $u->{statusvis} eq 'L';

    my $sess = LJ::Session->create($u, %$sess_opts);

    # return our hash
    return {
        ljsession => $sess->master_cookie_string,
    };
}

sub list_friends
{
    my ($u, $opts) = @_;

    # do not show people in here
    my %hide;  # userid -> 1

    # TAG:FR:protocol:list_friends
    my $sql;
    unless ($opts->{'friendof'}) {
        $sql = "SELECT friendid, fgcolor, bgcolor, groupmask FROM friends WHERE userid=?";
    } else {
        $sql = "SELECT userid FROM friends WHERE friendid=?";

        if (my $list = LJ::load_rel_user($u, 'B')) {
            $hide{$_} = 1 foreach @$list;
        }
    }

    my $dbr = LJ::get_db_reader();
    my $sth = $dbr->prepare($sql);
    $sth->execute($u->{'userid'});

    my @frow;
    while (my @row = $sth->fetchrow_array) {
        next if $hide{$row[0]};
        push @frow, [ @row ];
    }

    my $us = LJ::load_userids(map { $_->[0] } @frow);
    my $limitnum = $opts->{'limit'}+0;

    my $res = [];
    foreach my $f (sort { $us->{$a->[0]}{'user'} cmp $us->{$b->[0]}{'user'} }
                   grep { $us->{$_->[0]} } @frow)
    {
        my $u = $us->{$f->[0]};
        next if $opts->{'friendof'} && $u->{'statusvis'} ne 'V';

        my $r = {
            'username' => $u->{'user'},
            'fullname' => $u->{'name'},
        };


        if ($u->identity) {
            my $i = $u->identity;
            $r->{'identity_type'} = $i->pretty_type;
            $r->{'identity_value'} = $i->value;
            $r->{'identity_display'} = $u->display_name;
        }

        if ($opts->{'includebdays'} &&
            $u->{'bdate'} &&
            $u->{'bdate'} ne "0000-00-00" &&
            $u->can_show_full_bday)
        {
            $r->{'birthday'} = $u->{'bdate'};
        }

        unless ($opts->{'friendof'}) {
            $r->{'fgcolor'} = LJ::color_fromdb($f->[1]);
            $r->{'bgcolor'} = LJ::color_fromdb($f->[2]);
            $r->{"groupmask"} = $f->[3] if $f->[3] != 1;
        } else {
            $r->{'fgcolor'} = "#000000";
            $r->{'bgcolor'} = "#ffffff";
        }

        $r->{"type"} = {
            'C' => 'community',
            'Y' => 'syndicated',
            'N' => 'news',
            'S' => 'shared',
            'I' => 'identity',
        }->{$u->{'journaltype'}} if $u->{'journaltype'} ne 'P';

        $r->{"status"} = {
            'D' => "deleted",
            'S' => "suspended",
            'X' => "purged",
        }->{$u->{'statusvis'}} if $u->{'statusvis'} ne 'V';

        push @$res, $r;
        # won't happen for zero limit (which means no limit)
        last if @$res == $limitnum;
    }
    return $res;
}

sub syncitems
{
    my ($req, $err, $flags) = @_;
    return undef unless authenticate($req, $err, $flags);
    return undef unless check_altusage($req, $err, $flags);
    return fail($err,506) if $LJ::DISABLED{'syncitems'};

    my $ownerid = $flags->{'ownerid'};
    my $uowner = $flags->{'u_owner'} || $flags->{'u'};
    my $sth;

    my $db = LJ::get_cluster_reader($uowner);
    return fail($err,502) unless $db;

    ## have a valid date?
    my $date = $req->{'lastsync'};
    if ($date) {
        return fail($err,203,"Invalid date format")
            unless ($date =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d/);
    } else {
        $date = "0000-00-00 00:00:00";
    }

    my $LIMIT = 500;

    my %item;
    $sth = $db->prepare("SELECT jitemid, logtime FROM log2 WHERE ".
                        "journalid=? and logtime > ?");
    $sth->execute($ownerid, $date);
    while (my ($id, $dt) = $sth->fetchrow_array) {
        $item{$id} = [ 'L', $id, $dt, "create" ];
    }

    my %cmt;
    my $p_calter = LJ::get_prop("log", "commentalter");
    my $p_revtime = LJ::get_prop("log", "revtime");
    $sth = $db->prepare("SELECT jitemid, propid, FROM_UNIXTIME(value) ".
                        "FROM logprop2 WHERE journalid=? ".
                        "AND propid IN ($p_calter->{'id'}, $p_revtime->{'id'}) ".
                        "AND value+0 > UNIX_TIMESTAMP(?)");
    $sth->execute($ownerid, $date);
    while (my ($id, $prop, $dt) = $sth->fetchrow_array) {
        if ($prop == $p_calter->{'id'}) {
            $cmt{$id} = [ 'C', $id, $dt, "update" ];
        } elsif ($prop == $p_revtime->{'id'}) {
            $item{$id} = [ 'L', $id, $dt, "update" ];
        }
    }

    my @ev = sort { $a->[2] cmp $b->[2] } (values %item, values %cmt);

    my $res = {};
    my $list = $res->{'syncitems'} = [];
    $res->{'total'} = scalar @ev;
    my $ct = 0;
    while (my $ev = shift @ev) {
        $ct++;
        push @$list, { 'item' => "$ev->[0]-$ev->[1]",
                       'time' => $ev->[2],
                       'action' => $ev->[3],  };
        last if $ct >= $LIMIT;
    }
    $res->{'count'} = $ct;
    return $res;
}

sub consolecommand
{
    my ($req, $err, $flags) = @_;

    # logging in isn't necessary, but most console commands do require it
    LJ::set_remote($flags->{'u'}) if authenticate($req, $err, $flags);

    my $res = {};
    my $cmdout = $res->{'results'} = [];

    foreach my $cmd (@{$req->{'commands'}}) {
        # callee can pre-parse the args, or we can do it bash-style
        my @args = ref $cmd eq "ARRAY" ? @$cmd
                                       : LJ::Console->parse_line($cmd);
        my $c = LJ::Console->parse_array(@args);
        my $rv = $c->execute_safely;

        my @output;
        push @output, [$_->status, $_->text] foreach $c->responses;

        push @{$cmdout}, {
            'success' => $rv,
            'output' => \@output,
        };
    }

    return $res;
}

sub getchallenge
{
    my ($req, $err, $flags) = @_;
    my $res = {};
    my $now = time();
    my $etime = 60;
    $res->{'challenge'} = LJ::challenge_generate($etime);
    $res->{'server_time'} = $now;
    $res->{'expire_time'} = $now + $etime;
    $res->{'auth_scheme'} = "c0";  # fixed for now, might support others later
    return $res;
}

sub login_message
{
    my ($req, $res, $flags) = @_;
    my $u = $flags->{'u'};

    my $msg = sub {
        my $code = shift;
        my $args = shift || {};
        $args->{'sitename'} = $LJ::SITENAME;
        $args->{'siteroot'} = $LJ::SITEROOT;
        my $pre = delete $args->{'pre'};
        $res->{'message'} = $pre . translate($u, $code, $args);
    };

    return $msg->("readonly")          if LJ::get_cap($u, "readonly");
    return $msg->("not_validated")     if ($u->{'status'} eq "N" and not $LJ::EVERYONE_VALID);
    return $msg->("must_revalidate")   if ($u->{'status'} eq "T" and not $LJ::EVERYONE_VALID);

    my $checkpass = LJ::run_hook("bad_password", { 'u' => $u });
    return $msg->("bad_password", { 'pre' => "$checkpass " }) if $checkpass;

    return $msg->("old_win32_client")  if $req->{'clientversion'} =~ /^Win32-MFC\/(1.2.[0123456])$/;
    return $msg->("old_win32_client")  if $req->{'clientversion'} =~ /^Win32-MFC\/(1.3.[01234])\b/;
    return $msg->("hello_test")        if grep { $u->{user} eq $_ } @LJ::TESTACCTS;
}

sub list_friendgroups
{
    my $u = shift;

    # get the groups for this user, return undef if error
    my $groups = LJ::get_friend_group($u);
    return undef unless $groups;

    # we got all of the groups, so put them into an arrayref sorted by the
    # group sortorder; also note that the map is used to construct a new hashref
    # out of the old group hashref so that we have all of the field names converted
    # to a format our callers can recognize
    my @res = map { { id => $_->{groupnum},      name => $_->{groupname},
                      public => $_->{is_public}, sortorder => $_->{sortorder}, } }
              sort { $a->{sortorder} <=> $b->{sortorder} }
              values %$groups;

    return \@res;
}

sub list_usejournals {
    my $u = shift;

    my @us = $u->posting_access_list;
    my @unames = map { $_->{user} } @us;

    return \@unames;
}

sub hash_menus
{
    my $u = shift;
    my $user = $u->{'user'};

    my $menu = [
                { 'text' => "Recent Entries",
                  'url' => "$LJ::SITEROOT/users/$user/", },
                { 'text' => "Calendar View",
                  'url' => "$LJ::SITEROOT/users/$user/calendar", },
                { 'text' => "Friends View",
                  'url' => "$LJ::SITEROOT/users/$user/friends", },
                { 'text' => "-", },
                { 'text' => "Your Profile",
                  'url' => "$LJ::SITEROOT/userinfo.bml?user=$user", },
                { 'text' => "Your To-Do List",
                  'url' => "$LJ::SITEROOT/todo/?user=$user", },
                { 'text' => "-", },
                { 'text' => "Change Settings",
                  'sub' => [ { 'text' => "Personal Info",
                               'url' => "$LJ::SITEROOT/manage/profile/", },
                             { 'text' => "Customize Journal",
                               'url' =>"$LJ::SITEROOT/customize/", }, ] },
                { 'text' => "-", },
                { 'text' => "Support",
                  'url' => "$LJ::SITEROOT/support/", }
                ];

    LJ::run_hooks("modify_login_menu", {
        'menu' => $menu,
        'u' => $u,
        'user' => $user,
    });

    return $menu;
}

sub list_pickws
{
    my $u = shift;

    my $pi = LJ::get_userpic_info($u);
    my @res;

    my %seen;  # mashifiedptr -> 1

    # FIXME: should be a utf-8 sort
    foreach my $kw (sort keys %{$pi->{'kw'}}) {
        my $pic = $pi->{'kw'}{$kw};
        $seen{$pic} = 1;
        next if $pic->{'state'} eq "I";
        push @res, [ $kw, $pic->{'picid'} ];
    }

    # now add all the pictures that don't have a keyword
    foreach my $picid (keys %{$pi->{'pic'}}) {
        my $pic = $pi->{'pic'}{$picid};
        next if $seen{$pic};
        push @res, [ "pic#$picid", $picid ];
    }

    return \@res;
}

sub list_moods
{
    my $mood_max = int(shift);
    LJ::load_moods();

    my $res = [];
    return $res if $mood_max >= $LJ::CACHED_MOOD_MAX;

    for (my $id = $mood_max+1; $id <= $LJ::CACHED_MOOD_MAX; $id++) {
        next unless defined $LJ::CACHE_MOODS{$id};
        my $mood = $LJ::CACHE_MOODS{$id};
        next unless $mood->{'name'};
        push @$res, { 'id' => $id,
                      'name' => $mood->{'name'},
                      'parent' => $mood->{'parent'} };
    }

    return $res;
}

sub check_altusage
{
    my ($req, $err, $flags) = @_;

    # see note in ljlib.pl::can_use_journal about why we return
    # both 'ownerid' and 'u_owner' in $flags

    my $alt = $req->{'usejournal'};
    my $u = $flags->{'u'};
    unless ($u) {
        my $username = $req->{'username'};
        return fail($err,200) unless $username;
        return fail($err,100) unless LJ::canonical_username($username);

        my $dbr = LJ::get_db_reader();
        return fail($err,502) unless $dbr;
        $u = $flags->{'u'} = LJ::load_user($username);
    }

    $flags->{'ownerid'} = $u->{'userid'};

    # all good if not using an alt journal
    return 1 unless $alt;

    # complain if the username is invalid
    return fail($err,206) unless LJ::canonical_username($alt);

    my $r = eval { Apache->request };

    # allow usage if we're told explicitly that it's okay
    if ($flags->{'usejournal_okay'}) {
        $flags->{'u_owner'} = LJ::load_user($alt);
        $flags->{'ownerid'} = $flags->{'u_owner'}->{'userid'};
        $r->notes("journalid" => $flags->{'ownerid'}) if $r && !$r->notes("journalid");
        return 1 if $flags->{'ownerid'};
        return fail($err,206);
    }

    # otherwise, check for access:
    my $info = {};
    my $canuse = LJ::can_use_journal($u->{'userid'}, $alt, $info);
    $flags->{'ownerid'} = $info->{'ownerid'};
    $flags->{'u_owner'} = $info->{'u_owner'};
    $r->notes("journalid" => $flags->{'ownerid'}) if $r && !$r->notes("journalid");

    return 1 if $canuse || $flags->{'ignorecanuse'};

    # not allowed to access it
    return fail($err,300);
}

sub authenticate
{
    my ($req, $err, $flags) = @_;

    my $username = $req->{'username'};
    return fail($err,200) unless $username;
    return fail($err,100) unless LJ::canonical_username($username);

    my $u = $flags->{'u'};
    unless ($u) {
        my $dbr = LJ::get_db_reader();
        return fail($err,502) unless $dbr;
        $u = LJ::load_user($username);
    }

    return fail($err,100) unless $u;
    return fail($err,100) if ($u->{'statusvis'} eq "X");
    return fail($err,505) unless $u->{'clusterid'};

    my $r = eval { Apache->request };
    my $ip;
    if ($r) {
        $r->notes("ljuser" => $u->{'user'}) unless $r->notes("ljuser");
        $r->notes("journalid" => $u->{'userid'}) unless $r->notes("journalid");
        $ip = $r->connection->remote_ip;
    }

    my $ip_banned = 0;
    my $chal_expired = 0;
    my $auth_check = sub {

        my $auth_meth = $req->{'auth_method'} || "clear";
        if ($auth_meth eq "clear") {
            return LJ::auth_okay($u,
                                 $req->{'password'},
                                 $req->{'hpassword'},
                                 $u->password,
                                 \$ip_banned);
        }
        if ($auth_meth eq "challenge") {
            my $chal_opts = {};
            my $chall_ok = LJ::challenge_check_login($u,
                                                     $req->{'auth_challenge'},
                                                     $req->{'auth_response'},
                                                     \$ip_banned,
                                                     $chal_opts);
            $chal_expired = 1 if $chal_opts->{expired};
            return $chall_ok;
        }
        if ($auth_meth eq "cookie") {
            return unless $r && $r->header_in("X-LJ-Auth") eq "cookie";
            my $remote = LJ::get_remote();
            return $remote && $remote->{'user'} eq $username ? 1 : 0;
        }
    };

    unless ($flags->{'nopassword'} ||
            $flags->{'noauth'} ||
            $auth_check->() )
    {
        return fail($err,402) if $ip_banned;
        return fail($err,105) if $chal_expired;
        return fail($err,101);
    }

    # if there is a require TOS revision, check for it now
    return fail($err, 156, LJ::tosagree_str('protocol' => 'text'))
        unless $u->tosagree_verify;

    # remember the user record for later.
    $flags->{'u'} = $u;
    return 1;
}

sub fail
{
    my $err = shift;
    my $code = shift;
    my $des = shift;
    $code .= ":$des" if $des;
    $$err = $code if (ref $err eq "SCALAR");
    return undef;
}

# PROBLEM: a while back we used auto_increment fields in our tables so that we could have
# automatically incremented itemids and such.  this was eventually phased out in favor of
# the more portable alloc_user_counter function which uses the 'counter' table.  when the
# counter table has no data, it finds the highest id already in use in the database and adds
# one to it.
#
# a problem came about when users who last posted before alloc_user_counter went
# and deleted all their entries and posted anew.  alloc_user_counter would find no entries,
# this no ids, and thus assign id 1, thinking it's all clean and new.  but, id 1 had been
# used previously, and now has comments attached to it.
#
# the comments would happen because there was an old bug that wouldn't delete comments when
# an entry was deleted.  this has since been fixed.  so this all combines to make this
# a necessity, at least until no buggy data exist anymore!
#
# this code here removes any comments that happen to exist for the id we're now using.
sub new_entry_cleanup_hack {
    my ($u, $jitemid) = @_;

    # sanitize input
    $jitemid += 0;
    return unless $jitemid;
    my $ownerid = LJ::want_userid($u);
    return unless $ownerid;

    # delete logprops
    $u->do("DELETE FROM logprop2 WHERE journalid=$ownerid AND jitemid=$jitemid");

    # delete comments
    my $ids = LJ::Talk::get_talk_data($u, 'L', $jitemid);
    return unless ref $ids eq 'HASH' && %$ids;
    my $list = join ',', map { $_+0 } keys %$ids;
    $u->do("DELETE FROM talk2 WHERE journalid=$ownerid AND jtalkid IN ($list)");
    $u->do("DELETE FROM talktext2 WHERE journalid=$ownerid AND jtalkid IN ($list)");
    $u->do("DELETE FROM talkprop2 WHERE journalid=$ownerid AND jtalkid IN ($list)");
}

sub un_utf8_request {
    my $req = shift;
    $req->{$_} = LJ::no_utf8_flag($req->{$_}) foreach qw(subject event);
    my $props = $req->{props} || {};
    foreach my $k (keys %$props) {
        next if ref $props->{$k};  # if this is multiple levels deep?  don't think so.
        $props->{$k} = LJ::no_utf8_flag($props->{$k});
    }
}

#### Old interface (flat key/values) -- wrapper aruond LJ::Protocol
package LJ;

sub do_request
{
    # get the request and response hash refs
    my ($req, $res, $flags) = @_;

    # initialize some stuff
    %{$res} = ();                      # clear the given response hash
    $flags = {} unless (ref $flags eq "HASH");

    # did they send a mode?
    unless ($req->{'mode'}) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = "Client error: No mode specified.";
        return;
    }

    # this method doesn't require auth
    if ($req->{'mode'} eq "getchallenge") {
        return getchallenge($req, $res, $flags);
    }

    # mode from here on out require a username
    my $user = LJ::canonical_username($req->{'user'});
    unless ($user) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = "Client error: No username sent.";
        return;
    }

    ### see if the server's under maintenance now
    if ($LJ::SERVER_DOWN) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = $LJ::SERVER_DOWN_MESSAGE;
        return;
    }

    ## dispatch wrappers
    if ($req->{'mode'} eq "login") {
        return login($req, $res, $flags);
    }
    if ($req->{'mode'} eq "getfriendgroups") {
        return getfriendgroups($req, $res, $flags);
    }
    if ($req->{'mode'} eq "getfriends") {
        return getfriends($req, $res, $flags);
    }
    if ($req->{'mode'} eq "friendof") {
        return friendof($req, $res, $flags);
    }
    if ($req->{'mode'} eq "checkfriends") {
        return checkfriends($req, $res, $flags);
    }
    if ($req->{'mode'} eq "getdaycounts") {
        return getdaycounts($req, $res, $flags);
    }
    if ($req->{'mode'} eq "postevent") {
        return postevent($req, $res, $flags);
    }
    if ($req->{'mode'} eq "editevent") {
        return editevent($req, $res, $flags);
    }
    if ($req->{'mode'} eq "syncitems") {
        return syncitems($req, $res, $flags);
    }
    if ($req->{'mode'} eq "getevents") {
        return getevents($req, $res, $flags);
    }
    if ($req->{'mode'} eq "editfriends") {
        return editfriends($req, $res, $flags);
    }
    if ($req->{'mode'} eq "editfriendgroups") {
        return editfriendgroups($req, $res, $flags);
    }
    if ($req->{'mode'} eq "consolecommand") {
        return consolecommand($req, $res, $flags);
    }
    if ($req->{'mode'} eq "sessiongenerate") {
        return sessiongenerate($req, $res, $flags);
    }
    if ($req->{'mode'} eq "sessionexpire") {
        return sessionexpire($req, $res, $flags);
    }
    if ($req->{'mode'} eq "getusertags") {
        return getusertags($req, $res, $flags);
    }
    if ($req->{'mode'} eq "getfriendspage") {
        return getfriendspage($req, $res, $flags);
    }

    ### unknown mode!
    $res->{'success'} = "FAIL";
    $res->{'errmsg'} = "Client error: Unknown mode ($req->{'mode'})";
    return;
}

## flat wrapper
sub getfriendspage
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("getfriendspage", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    my $ect = 0;
    foreach my $evt (@{$rs->{'entries'}}) {
        $ect++;
        foreach my $f (qw(subject_raw journalname journaltype postername postertype ditemid security)) {
            if (defined $evt->{$f}) {
                $res->{"entries_${ect}_$f"} = $evt->{$f};
            }
        }
        $res->{"entries_${ect}_event"} = LJ::eurl($evt->{'event_raw'});
    }

    $res->{'entries_count'} = $ect;
    $res->{'success'} = "OK";

    return 1;
}

## flat wrapper
sub login
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("login", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    $res->{'name'} = $rs->{'fullname'};
    $res->{'message'} = $rs->{'message'} if $rs->{'message'};
    $res->{'fastserver'} = 1 if $rs->{'fastserver'};
    $res->{'caps'} = $rs->{'caps'} if $rs->{'caps'};

    # shared journals
    my $access_count = 0;
    foreach my $user (@{$rs->{'usejournals'}}) {
        $access_count++;
        $res->{"access_${access_count}"} = $user;
    }
    if ($access_count) {
        $res->{"access_count"} = $access_count;
    }

    # friend groups
    populate_friend_groups($res, $rs->{'friendgroups'});

    my $flatten = sub {
        my ($prefix, $listref) = @_;
        my $ct = 0;
        foreach (@$listref) {
            $ct++;
            $res->{"${prefix}_$ct"} = $_;
        }
        $res->{"${prefix}_count"} = $ct;
    };

    ### picture keywords
    $flatten->("pickw", $rs->{'pickws'})
        if defined $req->{"getpickws"};
    $flatten->("pickwurl", $rs->{'pickwurls'})
        if defined $req->{"getpickwurls"};
    $res->{'defaultpicurl'} = $rs->{'defaultpicurl'} if $rs->{'defaultpicurl'};

    ### report new moods that this client hasn't heard of, if they care
    if (defined $req->{"getmoods"}) {
        my $mood_count = 0;
        foreach my $m (@{$rs->{'moods'}}) {
            $mood_count++;
            $res->{"mood_${mood_count}_id"} = $m->{'id'};
            $res->{"mood_${mood_count}_name"} = $m->{'name'};
            $res->{"mood_${mood_count}_parent"} = $m->{'parent'};
        }
        if ($mood_count) {
            $res->{"mood_count"} = $mood_count;
        }
    }

    #### send web menus
    if ($req->{"getmenus"} == 1) {
        my $menu = $rs->{'menus'};
        my $menu_num = 0;
        populate_web_menu($res, $menu, \$menu_num);
    }

    return 1;
}

## flat wrapper
sub getfriendgroups
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("getfriendgroups", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }
    $res->{'success'} = "OK";
    populate_friend_groups($res, $rs->{'friendgroups'});

    return 1;
}

## flat wrapper
sub getusertags
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("getusertags", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";

    my $ct = 0;
    foreach my $tag (@{$rs->{tags}}) {
        $ct++;
        $res->{"tag_${ct}_security"} = $tag->{security_level};
        $res->{"tag_${ct}_uses"} = $tag->{uses} if $tag->{uses};
        $res->{"tag_${ct}_display"} = $tag->{display} if $tag->{display};
        $res->{"tag_${ct}_name"} = $tag->{name};
        foreach my $lev (qw(friends private public)) {
            $res->{"tag_${ct}_sb_$_"} = $tag->{security}->{$_}
                if $tag->{security}->{$_};
        }
        my $gm = 0;
        foreach my $grpid (keys %{$tag->{security}->{groups}}) {
            next unless $tag->{security}->{groups}->{$grpid};
            $gm++;
            $res->{"tag_${ct}_sb_group_${gm}_id"} = $grpid;
            $res->{"tag_${ct}_sb_group_${gm}_count"} = $tag->{security}->{groups}->{$grpid};
        }
        $res->{"tag_${ct}_sb_group_count"} = $gm if $gm;
    }
    $res->{'tag_count'} = $ct;

    return 1;
}

## flat wrapper
sub getfriends
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("getfriends", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    if ($req->{'includegroups'}) {
        populate_friend_groups($res, $rs->{'friendgroups'});
    }
    if ($req->{'includefriendof'}) {
        populate_friends($res, "friendof", $rs->{'friendofs'});
    }
    populate_friends($res, "friend", $rs->{'friends'});

    return 1;
}

## flat wrapper
sub friendof
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("friendof", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    populate_friends($res, "friendof", $rs->{'friendofs'});
    return 1;
}

## flat wrapper
sub checkfriends
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("checkfriends", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    $res->{'new'} = $rs->{'new'};
    $res->{'lastupdate'} = $rs->{'lastupdate'};
    $res->{'interval'} = $rs->{'interval'};
    return 1;
}

## flat wrapper
sub getdaycounts
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("getdaycounts", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    foreach my $d (@{ $rs->{'daycounts'} }) {
        $res->{$d->{'date'}} = $d->{'count'};
    }
    return 1;
}

## flat wrapper
sub syncitems
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("syncitems", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    $res->{'sync_total'} = $rs->{'total'};
    $res->{'sync_count'} = $rs->{'count'};

    my $ct = 0;
    foreach my $s (@{ $rs->{'syncitems'} }) {
        $ct++;
        foreach my $a (qw(item action time)) {
            $res->{"sync_${ct}_$a"} = $s->{$a};
        }
    }
    return 1;
}

## flat wrapper: limited functionality.  (1 command only, server-parsed only)
sub consolecommand
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);
    delete $rq->{'command'};

    $rq->{'commands'} = [ $req->{'command'} ];

    my $rs = LJ::Protocol::do_request("consolecommand", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'cmd_success'} = $rs->{'results'}->[0]->{'success'};
    $res->{'cmd_line_count'} = 0;
    foreach my $l (@{$rs->{'results'}->[0]->{'output'}}) {
        $res->{'cmd_line_count'}++;
        my $line = $res->{'cmd_line_count'};
        $res->{"cmd_line_${line}_type"} = $l->[0]
            if $l->[0];
        $res->{"cmd_line_${line}"} = $l->[1];
    }

    $res->{'success'} = "OK";

}

## flat wrapper
sub getchallenge
{
    my ($req, $res, $flags) = @_;
    my $err = 0;
    my $rs = LJ::Protocol::do_request("getchallenge", $req, \$err, $flags);

    # stupid copy (could just return $rs), but it might change in the future
    # so this protects us from future accidental harm.
    foreach my $k (qw(challenge server_time expire_time auth_scheme)) {
        $res->{$k} = $rs->{$k};
    }

    $res->{'success'} = "OK";
    return $res;
}

## flat wrapper
sub editfriends
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    $rq->{'add'} = [];
    $rq->{'delete'} = [];

    foreach (keys %$req) {
        if (/^editfriend_add_(\d+)_user$/) {
            my $n = $1;
            next unless ($req->{"editfriend_add_${n}_user"} =~ /\S/);
            my $fa = { 'username' => $req->{"editfriend_add_${n}_user"},
                       'fgcolor' => $req->{"editfriend_add_${n}_fg"},
                       'bgcolor' => $req->{"editfriend_add_${n}_bg"},
                       'groupmask' => $req->{"editfriend_add_${n}_groupmask"},
                   };
            push @{$rq->{'add'}}, $fa;
        } elsif (/^editfriend_delete_(\w+)$/) {
            push @{$rq->{'delete'}}, $1;
        }
    }

    my $rs = LJ::Protocol::do_request("editfriends", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";

    my $ct = 0;
    foreach my $fa (@{ $rs->{'added'} }) {
        $ct++;
        $res->{"friend_${ct}_user"} = $fa->{'username'};
        $res->{"friend_${ct}_name"} = $fa->{'fullname'};
    }

    $res->{'friends_added'} = $ct;

    return 1;
}

## flat wrapper
sub editfriendgroups
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    $rq->{'groupmasks'} = {};
    $rq->{'set'} = {};
    $rq->{'delete'} = [];

    foreach (keys %$req) {
        if (/^efg_set_(\d+)_name$/) {
            next unless ($req->{$_} ne "");
            my $n = $1;
            my $fs = {
                'name' => $req->{"efg_set_${n}_name"},
                'sort' => $req->{"efg_set_${n}_sort"},
            };
            if (defined $req->{"efg_set_${n}_public"}) {
                $fs->{'public'} = $req->{"efg_set_${n}_public"};
            }
            $rq->{'set'}->{$n} = $fs;
        }
        elsif (/^efg_delete_(\d+)$/) {
            if ($req->{$_}) {
                # delete group if value is true
                push @{$rq->{'delete'}}, $1;
            }
        }
        elsif (/^editfriend_groupmask_(\w+)$/) {
            $rq->{'groupmasks'}->{$1} = $req->{$_};
        }
    }

    my $rs = LJ::Protocol::do_request("editfriendgroups", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    return 1;
}

sub flatten_props
{
    my ($req, $rq) = @_;

    ## changes prop_* to props hashref
    foreach my $k (keys %$req) {
        next unless ($k =~ /^prop_(.+)/);
        $rq->{'props'}->{$1} = $req->{$k};
    }
}

## flat wrapper
sub postevent
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);
    flatten_props($req, $rq);
    $rq->{'props'}->{'interface'} = "flat";

    my $rs = LJ::Protocol::do_request("postevent", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'message'} = $rs->{'message'} if $rs->{'message'};
    $res->{'extra_result_message'} = $rs->{'extra_result_message'} if $rs->{'extra_result_message'};
    $res->{'success'} = "OK";
    $res->{'itemid'} = $rs->{'itemid'};
    $res->{'anum'} = $rs->{'anum'} if defined $rs->{'anum'};
    $res->{'url'} = $rs->{'url'} if defined $rs->{'url'};
    return 1;
}

## flat wrapper
sub editevent
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);
    flatten_props($req, $rq);

    my $rs = LJ::Protocol::do_request("editevent", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    $res->{'success'} = "OK";
    $res->{'itemid'} = $rs->{'itemid'};
    $res->{'anum'} = $rs->{'anum'} if defined $rs->{'anum'};
    $res->{'url'} = $rs->{'url'} if defined $rs->{'url'};
    return 1;
}

## flat wrapper
sub sessiongenerate {
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request('sessiongenerate', $rq, \$err, $flags);
    unless ($rs) {
        $res->{success} = 'FAIL';
        $res->{errmsg} = LJ::Protocol::error_message($err);
    }

    $res->{success} = 'OK';
    $res->{ljsession} = $rs->{ljsession};
    return 1;
}

## flat wrappre
sub sessionexpire {
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    $rq->{expire} = [];
    foreach my $k (keys %$rq) {
        push @{$rq->{expire}}, $1
            if $k =~ /^expire_id_(\d+)$/;
    }

    my $rs = LJ::Protocol::do_request('sessionexpire', $rq, \$err, $flags);
    unless ($rs) {
        $res->{success} = 'FAIL';
        $res->{errmsg} = LJ::Protocol::error_message($err);
    }

    $res->{success} = 'OK';
    return 1;
}

## flat wrapper
sub getevents
{
    my ($req, $res, $flags) = @_;

    my $err = 0;
    my $rq = upgrade_request($req);

    my $rs = LJ::Protocol::do_request("getevents", $rq, \$err, $flags);
    unless ($rs) {
        $res->{'success'} = "FAIL";
        $res->{'errmsg'} = LJ::Protocol::error_message($err);
        return 0;
    }

    my $ect = 0;
    my $pct = 0;
    foreach my $evt (@{$rs->{'events'}}) {
        $ect++;
        foreach my $f (qw(itemid eventtime security allowmask subject anum url poster)) {
            if (defined $evt->{$f}) {
                $res->{"events_${ect}_$f"} = $evt->{$f};
            }
        }
        $res->{"events_${ect}_event"} = LJ::eurl($evt->{'event'});

        if ($evt->{'props'}) {
            foreach my $k (sort keys %{$evt->{'props'}}) {
                $pct++;
                $res->{"prop_${pct}_itemid"} = $evt->{'itemid'};
                $res->{"prop_${pct}_name"} = $k;
                $res->{"prop_${pct}_value"} = $evt->{'props'}->{$k};
            }
        }
    }

    unless ($req->{'noprops'}) {
        $res->{'prop_count'} = $pct;
    }
    $res->{'events_count'} = $ect;
    $res->{'success'} = "OK";

    return 1;
}


sub populate_friends
{
    my ($res, $pfx, $list) = @_;
    my $count = 0;
    foreach my $f (@$list)
    {
        $count++;
        $res->{"${pfx}_${count}_name"} = $f->{'fullname'};
        $res->{"${pfx}_${count}_user"} = $f->{'username'};
        $res->{"${pfx}_${count}_birthday"} = $f->{'birthday'} if $f->{'birthday'};
        $res->{"${pfx}_${count}_bg"} = $f->{'bgcolor'};
        $res->{"${pfx}_${count}_fg"} = $f->{'fgcolor'};
        if (defined $f->{'groupmask'}) {
            $res->{"${pfx}_${count}_groupmask"} = $f->{'groupmask'};
        }
        if (defined $f->{'type'}) {
            $res->{"${pfx}_${count}_type"} = $f->{'type'};
            if ($f->{'type'} eq 'identity') {
                $res->{"${pfx}_${count}_identity_type"}    = $f->{'identity_type'};
                $res->{"${pfx}_${count}_identity_value"}   = $f->{'identity_value'};
                $res->{"${pfx}_${count}_identity_display"} = $f->{'identity_display'};
            }
        }
        if (defined $f->{'status'}) {
            $res->{"${pfx}_${count}_status"} = $f->{'status'};
        }
    }
    $res->{"${pfx}_count"} = $count;
}


sub upgrade_request
{
    my $r = shift;
    my $new = { %{ $r } };
    $new->{'username'} = $r->{'user'};

    # but don't delete $r->{'user'}, as it might be, say, %FORM,
    # that'll get reused in a later request in, say, update.bml after
    # the login before postevent.  whoops.

    return $new;
}

## given a $res hashref and friend group subtree (arrayref), flattens it
sub populate_friend_groups
{
    my ($res, $fr) = @_;

    my $maxnum = 0;
    foreach my $fg (@$fr)
    {
        my $num = $fg->{'id'};
        $res->{"frgrp_${num}_name"} = $fg->{'name'};
        $res->{"frgrp_${num}_sortorder"} = $fg->{'sortorder'};
        if ($fg->{'public'}) {
            $res->{"frgrp_${num}_public"} = 1;
        }
        if ($num > $maxnum) { $maxnum = $num; }
    }
    $res->{'frgrp_maxnum'} = $maxnum;
}

## given a menu tree, flattens it into $res hashref
sub populate_web_menu
{
    my ($res, $menu, $numref) = @_;
    my $mn = $$numref;  # menu number
    my $mi = 0;         # menu item
    foreach my $it (@$menu) {
        $mi++;
        $res->{"menu_${mn}_${mi}_text"} = $it->{'text'};
        if ($it->{'text'} eq "-") { next; }
        if ($it->{'sub'}) {
            $$numref++;
            $res->{"menu_${mn}_${mi}_sub"} = $$numref;
            &populate_web_menu($res, $it->{'sub'}, $numref);
            next;

        }
        $res->{"menu_${mn}_${mi}_url"} = $it->{'url'};
    }
    $res->{"menu_${mn}_count"} = $mi;
}

1;
