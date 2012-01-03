#!/usr/bin/env python
# 
#  This script fetches data from livejournal friendspage via public XML-RPC api
#  and formats Atom feed based on the data.
#
#  Copyright (C) 2010  Leonid Evdokimov <leon@darkk.net.ru>
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
# 
#  Origin:
#  http://github.com/darkk/ljfeed/
#
#  Changelog:
#  2010.01.24 - Initial revision.


import datetime
import hashlib
import re
import uuid
import xmlrpclib
import xml.sax.saxutils as saxutils
from optparse import OptionParser
import os
import sys
import tempfile


DEBUG = False


def create_transport(ljuser):
    UA = 'FP-bot, for private usage only (http://%s.livejournal.com; %s@livejournal.com)' % (ljuser, ljuser)
    class LJGaswagen(xmlrpclib.Transport):
        user_agent = UA
    return LJGaswagen()


def getfriendspage(ljuser, password=None, pass_md5=None):
    if pass_md5 is None:
        assert password is not None
        pass_md5 = hashlib.md5(password).hexdigest()
    req = {'username': ljuser,
           'hpassword': pass_md5,
           'auth_method': 'clear',
           'ver': 1}
    # use_datetime does not help as LJ sends <int/> instead of some sort of <date/>
    proxy = xmlrpclib.ServerProxy('http://livejournal.com/interface/xmlrpc',
                                  transport=create_transport(ljuser),
                                  verbose=DEBUG)
    friendspage = proxy.LJ.XMLRPC.getfriendspage(req)
    return friendspage


def fmt_atom_time(timestamp):
    d = datetime.datetime.utcfromtimestamp(timestamp)
    return '%04i-%02i-%02iT%02i:%02i:%02iZ' % (d.year, d.month, d.day, d.hour, d.minute, d.second)


def fmt_ljevent_raw(event_raw):
    event_raw = str(event_raw)

    if '<br />' not in event_raw:
        event_raw = event_raw.replace('\n', '<br />')

    event_raw = re.sub(r'</?lj-cut[^>]*>', ' ', event_raw)

    def fmt_ljuser(match):
        ljuser = match.group(1).replace('_', '-')
        return ''.join(('<a href="http://', ljuser, '.livejournal.com/">',
                        '<img src="http://l-stat.livejournal.com/img/userinfo.gif"/>',
                        ljuser, '</a>'))
    event_raw = re.sub(r'<lj\s+user="?([a-zA-Z0-9_-]+)"?.*?>', fmt_ljuser, event_raw)

    def fmt_ljcomm(match):
        ljcomm = match.group(1)
        return ''.join(('<a href="http://community.livejournal.com/', ljcomm, '/">'
                        '<img src="http://l-stat.livejournal.com/img/community.gif"/></a>'))
    event_raw = re.sub(r'<lj\s+comm="?([a-zA-Z0-9_-]+)"?.*?>', fmt_ljcomm, event_raw)

    return event_raw

def is_private(entry):
    return entry['security'] != 'public'


def fmt_title(entry):
    warning_mark = '\xe2\x9a\xa0 ' # \u26a0 + SPACE
    postername = entry['postername']
    journalname = entry['journalname']
    subject_raw = str(entry['subject_raw']) # it may be xmlrpclib.Binary instance
    if not subject_raw:
        subject_raw = '(no subject)'
    private_mark = warning_mark if is_private(entry) else ''
    if postername == journalname:
        return "%s / %s%s" % (postername, private_mark, subject_raw)
    else:
        return "%s @ %s / %s%s" % (postername, journalname, private_mark, subject_raw)


def fmt_feed(ljuser, friendspage_entries):
    """ returns: (feed_data, mtime) """
    FEED_PREFIX = """<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
        <title>%(_ljuser)s's friends</title>
        <updated>%(_mtime)s</updated>
        <author><name>%(_ljuser)s and his friends</name></author>
        <id>urn:uuid:%(_random_uuid)s</id>
    """
    FEED_ENTRY = """
        <entry>
            <title>%(_title)s</title>
            <link href="%(journalurl)s/%(ditemid)s.html" rel="alternate"/>
            <id>%(journalurl)s/%(ditemid)s.html</id>
            <updated>%(_logtime)s</updated>
            <author><name>%(postername)s</name></author>
            <summary type="html">
                %(_event_raw)s
            </summary>
        </entry>
    """
    FEED_POSTFIX = """
    </feed>
    """

    feed = []

    mtime = max(e['logtime'] for e in friendspage_entries)
    feed.append(FEED_PREFIX % {'_ljuser': ljuser,
                               '_random_uuid': uuid.uuid4(),
                               '_mtime': fmt_atom_time(mtime)})

    for entry in friendspage_entries:
        same_keys = ('subject_raw', 'journalurl', 'ditemid', 'postername', 'journalname')
        vars = dict( (key, entry[key]) for key in same_keys )
        vars['_logtime'] = fmt_atom_time(entry['logtime'])
        vars['_event_raw'] = fmt_ljevent_raw(entry['event_raw'])
        vars['_title'] = fmt_title(entry)
        for k in vars.keys():
            vars[k] = saxutils.escape(str(vars[k]))
        feed.append(FEED_ENTRY % vars)

    feed.append(FEED_POSTFIX)

    return ''.join(feed), mtime

def write_to(output, pair):
    feed, mtime = pair
    if os.path.isfile(output) and os.path.getmtime(output) > mtime:
        if DEBUG:
            print >>sys.stderr, 'No new entries since', output, 'mtime, leaving it intact.'
        return
    # mkstemp creates file with 600, I need (666 & umask), that's why I use mktemp instead of mkstemp
    tmp = tempfile.mktemp(dir=os.path.dirname(output), prefix=os.path.basename(output))
    if DEBUG:
        print >>sys.stderr, 'Using', tmp, 'as temp file'
    with open(tmp, 'w') as fd:
        fd.write(feed)
    os.rename(tmp, output)

def main():
    parser = OptionParser()
    parser.add_option('-u', '--user', help='Log in as USER')
    parser.add_option('-p', '--password', help='Log in with PASSWORD')
    parser.add_option('-P', '--pass_md5', help='Use MD5(password) instead of PASSWORD')
    parser.add_option('-O', '--output', help='Write full (public+private) feed to OUTPUT file')
    parser.add_option('-a', '--public', help='Write public feed to PUBLIC file')
    parser.add_option('-x', '--private', help='Write private-only feed to PRIVATE file')
    parser.add_option('-v', '--verbose', help='Turn on debugging', action='store_true')
    opt, args = parser.parse_args()

    if not opt.user or not (opt.password or opt.pass_md5):
        parser.error("I need both --user and some form of password (--password or --pass_md5)")
    if not (opt.output or opt.public or opt.private):
        parser.error("I need at least one output (--output, --public or --private)")
    if opt.verbose:
        global DEBUG
        DEBUG = True

    friendspage = getfriendspage(opt.user, opt.password, opt.pass_md5)
    entries = friendspage['entries']

    if opt.output:
        write_to(opt.output, fmt_feed(opt.user, entries))
    if opt.public:
        write_to(opt.public, fmt_feed(opt.user, filter(lambda x: not is_private(x), entries)))
    if opt.private:
        write_to(opt.private, fmt_feed(opt.user, filter(is_private, entries)))

if __name__ == '__main__':
    main()

# vim:set tabstop=4 softtabstop=4 shiftwidth=4 expandtab: 
