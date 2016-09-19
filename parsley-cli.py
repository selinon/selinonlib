#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################


import argparse
from parsley import Logger, System
from parsley import parsley_version as version
from parsley import Config


def main():
    parser = argparse.ArgumentParser('parsley',
                                     description='YAML configuration file parser for Celeriac; v%s' % version)
    parser.add_argument('-config', dest='config', action='store', metavar='CONFIG.yml',
                        help='path to configuration file')
    parser.add_argument('-nodes-definition', dest='nodes', action='store', metavar='NODES.yml',
                        help='path to tasks definition file', required=True)
    parser.add_argument('-flow-definition', dest='flows', action='store', metavar='FLOW.yml',
                        help='path to flow definition file', required=True, nargs='+')
    parser.add_argument('-verbose', dest='verbose', action='count')

    parser.add_argument('-dump', dest='dump', action='store', metavar='DUMP.py',
                        help='generate Python code of the system')
    parser.add_argument('-no-check', dest='no_check', action='store_true',
                        help='do not check system for errors')
    parser.add_argument('-graph', dest='graph', action='store', metavar='OUTPUT_DIR',
                        help='construct an image of the system')
    parser.add_argument('-graph-format', dest='graph_format', action='store', metavar='FORMAT', default='svg',
                        help='format of the output image for -graph')
    parser.add_argument('-list-task-queues', dest='list_task_queues', action='store_true',
                        help='list all task queues to stdout')
    parser.add_argument('-list-dispatcher-queue', dest='list_dispatcher_queue', action='store_true',
                        help='list dispatcher queue to stdout')

    args = parser.parse_args()

    Logger.set_verbosity(args.verbose)
    Config.set_config(args.config)

    system = System.from_files(args.nodes, args.flows, args.no_check)

    some_work = False
    if args.dump:
        system.dump2file(args.dump)
        some_work = True

    if args.graph:
        system.plot_graph(args.graph, args.graph_format)
        some_work = True

    if args.list_task_queues:
        for task_name, queue_name in system.task_queue_names().items():
            print("%s:%s" % (task_name, queue_name))
            some_work = True

    if args.list_dispatcher_queue:
        print("%s:%s" % ('dispatcher', system.dispatcher_queue_name()))
        some_work = True

    if not some_work:
        parser.print_help()
        return 1

if __name__ == "__main__":
    main()
