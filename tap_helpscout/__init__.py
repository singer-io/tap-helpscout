import singer

from tap_helpscout.client import HelpScoutClient
from tap_helpscout.discover import discover
from tap_helpscout.sync import sync

LOGGER = singer.get_logger()

# These are the required keys to be present in the configuration json
REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "refresh_token", "user_agent"]


def do_discover():
    """Starts discovery process."""
    LOGGER.info("Starting discover")
    catalog = discover()
    catalog.dump()
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if parsed_args.dev:
        LOGGER.warning("Executing tap in dev mode")

    with HelpScoutClient(parsed_args.config_path, parsed_args.config, parsed_args.dev) as helpscout_client:

        state = parsed_args.state or {}
        if parsed_args.discover:
            do_discover()
        else:
            state = parsed_args.state or {}
            sync(
                client=helpscout_client,
                catalog=parsed_args.catalog or discover(),
                state=state or {},
                start_date=parsed_args.config["start_date"],
            )


if __name__ == "__main__":
    main()
