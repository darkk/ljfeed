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
        return ''.join(('<a href="', ljuser, '.livejournal.com/">',
                        '<img src="http://l-stat.livejournal.com/img/userinfo.gif"/>',
                        ljuser, '</a>'))
    event_raw = re.sub(r'<lj\s+user="?([a-zA-Z0-9_-]+)"?.*?>', fmt_ljuser, event_raw)

    def fmt_ljcomm(match):
        ljcomm = match.group(1)
        return ''.join(('<a href="http://community.livejournal.com/', ljcomm, '/">'
                        '<img src="http://l-stat.livejournal.com/img/community.gif"/></a>'))
    event_raw = re.sub(r'<lj\s+comm="?([a-zA-Z0-9_-]+)"?.*?>', fmt_ljcomm, event_raw)

    return event_raw


def fmt_feed(ljuser, friendspage):
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
            <title>%(subject_raw)s</title>
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

    mtime = max(e['logtime'] for e in friendspage['entries'])
    feed.append(FEED_PREFIX % {'_ljuser': ljuser,
                               '_random_uuid': uuid.uuid4(),
                               '_mtime': fmt_atom_time(mtime)})

    for entry in friendspage['entries']:
        same_keys = ('subject_raw', 'journalurl', 'ditemid', 'postername')
        vars = dict( (key, entry[key]) for key in same_keys )
        vars['_logtime'] = fmt_atom_time(entry['logtime'])
        vars['_event_raw'] = fmt_ljevent_raw(entry['event_raw'])
        for k in vars.keys():
            vars[k] = saxutils.escape(str(vars[k]))
        feed.append(FEED_ENTRY % vars)

    feed.append(FEED_POSTFIX)

    return ''.join(feed), mtime


def main():
    from getopt import getopt
    import sys, os
    ljuser, password, pass_md5, output = None, None, None, None

    opts, extra_args = getopt(sys.argv[1:], 'u:p:P:O:', ['user=', 'password=', 'pass_md5=', 'output='])
    for key, value in opts:
        if key in ('-u', '--user'):
            ljuser = value
        elif key in ('-O', '--output'):
            output = value
        elif key in ('-p', '--password'):
            password = value
        elif key in ('-P', '--pass_md5'):
            pass_md5 = value

    if not ljuser or (not password and not pass_md5):
        print "Usage: %s --user <ljuser> (--password <p@s$w0rd>|--pass_md5 <ab84...7c>) [--output ljuser.xml] " % sys.argv[0]
        sys.exit(1)

    if not output:
        output = ljuser + '.xml'

    friendspage = getfriendspage(ljuser, password=password, pass_md5=pass_md5)
    feed, mtime = fmt_feed(ljuser, friendspage)

    if os.path.isfile(output):
        if os.path.getmtime(output) > mtime:
            return
        else:
            os.unlink(output)
    with open(output, 'w') as fd:
        fd.write(feed)
    return


if __name__ == '__main__':
    main()


# vim:set tabstop=4 softtabstop=4 shiftwidth=4 expandtab: 
