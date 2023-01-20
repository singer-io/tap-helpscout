#!/usr/bin/env python3

import sys
import json
import singer
from tap_helpscout.client import HelpScoutClient
from tap_helpscout.discover import discover
from tap_helpscout.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'refresh_token',
    'user_agent'
]

def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if parsed_args.dev:
        LOGGER.warning("Executing tap in dev mode")

    with HelpScoutClient(parsed_args.config_path,
                         parsed_args.config,
                         parsed_args.dev) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client=client,
                 catalog=parsed_args.catalog,
                 state=state,
                 start_date=parsed_args.config['start_date'])

if __name__ == '__main__':
    main()
