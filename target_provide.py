#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import prvd
import threading
import http.client
import urllib
from datetime import datetime
import collections
import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def process_messages(messages):
    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    for message in messages:
        try:
            msg = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("ERROR: Failed to parse message\n{}".format(message))
        message_type = msg['type']
        if message_type == 'RECORD':
            if msg['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before schema".format(msg['stream']))
            
            validators[msg['stream']].validate(msg['record'])

            
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(msg['value']))
        elif message_type == 'SCHEMA':
            stream = msg['stream']
            schemas[stream] = msg['schema']
            validators[stream] = Draft4Validator(msg['schema'])
            key_properties[stream] = msg['key_properties']
        else:
            logger.warning("Unknown message type {} in message {}".format(msg['type'],msg))
    
    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-provide').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-provide',
            'se_ac': 'open',
            'se_ta': version,
        }
        conn.request('GET','/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
            'To disable sending anonymous usage data, set ' +
            'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = process_messages(input_messages)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()