#!/usr/bin/env python

import argparse
import yaml
from parsley import Logger, Task, Flow, System, Edge
from parsley import parsley_version as version
from parsley import Config


def _system_from_args(args):
    logger = Logger.get_logger(__name__)
    system = System()

    with open(args.tasks, 'r') as f:
        logger.debug("Parsing '{}'".format(args.tasks))
        try:
            content = yaml.load(f)
        except:
            logger.error("Bad YAML file, unable to load tasks from {}".format(args.tasks))
            raise

    for task_dict in content['tasks']:
        task = Task.from_dict(task_dict)
        system.add_task(task)

    with open(args.flow, 'r') as f:
        logger.debug("Parsing '{}'".format(args.flow))
        try:
            content = yaml.load(f)
        except:
            logger.error("Bad YAML file, unable to load flow from {}".format(args.flow))
            raise

    for flow_name in content['flows']:
        flow = Flow(flow_name)
        system.add_flow(flow)

    for flow_def in content['flow-definitions']:
        flow = system.flow_by_name(flow_def['name'])
        for edge_def in flow_def['edges']:
            edge = Edge.from_dict(edge_def, system)
            flow.add_edge(edge)

    system.post_parse_check()
    return system


def main():
    parser = argparse.ArgumentParser('parsley',
                                     description='YAML configuration file parser for Celeriac; v%s' % version)
    parser.add_argument('-config', dest='config', action='store', metavar='CONFIG.yml',
                        help='path to configuration file')
    parser.add_argument('-tasks-definition', dest='tasks', action='store', metavar='TASKS.yml',
                        help='path to tasks definition file', required=True)
    parser.add_argument('-flow-definition', dest='flow', action='store', metavar='FLOW.yml',
                        help='path to flow definition file', required=True)
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

    Config.set_config(args.config)
    Logger.set_verbosity(args.verbose)

    system = _system_from_args(args)

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
