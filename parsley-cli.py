#!/usr/bin/env python

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

    args = parser.parse_args()

    Logger.set_verbosity(args.verbose)
    Config.set_config(args.config)

    system = System.from_files(args.nodes, args.flows)

    some_work = False

    if not args.no_check:
        system.check()
        some_work = True

    if args.dump:
        system.dump(args.dump)
        some_work = True
    elif args.graph:
        system.plot_graph(args.graph, args.graph_format)
        some_work = True

    if not some_work:
        parser.print_help()
        return 1

if __name__ == "__main__":
    main()
