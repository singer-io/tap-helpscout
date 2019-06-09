# tap-helpscout

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [HelpScout Mailbox API](https://developer.helpscout.com/mailbox-api/)
- Extracts the following resources:
  - [Conversations](https://developer.helpscout.com/mailbox-api/endpoints/conversations/list/)
    - [Conversation Threads](https://developer.helpscout.com/mailbox-api/endpoints/conversations/threads/list/)
  - [Customers](https://developer.helpscout.com/mailbox-api/endpoints/customers/list/)
  - [Mailboxes](https://developer.helpscout.com/mailbox-api/endpoints/mailboxes/list/)
    - [Mailbox Fields](https://developer.helpscout.com/mailbox-api/endpoints/mailboxes/mailbox-fields/)
    - [Mailbox Folders](https://developer.helpscout.com/mailbox-api/endpoints/mailboxes/mailbox-folders/)
  - [Users](https://developer.helpscout.com/mailbox-api/endpoints/users/list/)
  - [Workflows](https://developer.helpscout.com/mailbox-api/endpoints/workflows/list/)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-helpscout
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)
3. Create your tap's config file which should look like the following:

    ```json
    {
        "client_id": "OAUTH_CLIENT_ID",
        "client_secret": "OAUTH_CLIENT_SECRET",
        "refresh_token": "YOUR_OAUTH_REFRESH_TOKEN",
        "start_date": "2017-04-19T13:37:30Z"
    }
    ```

4. [Optional] Create the initial state file

    ```json
    {
        "conversations": "2000-01-01T00:00:00Z",
        "conversation_threads": "2000-01-01T00:00:00Z",
        "customers": "2000-01-01T00:00:00Z",
        "mailboxes": "2000-01-01T00:00:00Z",
        "mailbox_fields": "2000-01-01T00:00:00Z",
        "mailbox_folders": "2000-01-01T00:00:00Z",
        "users": "2000-01-01T00:00:00Z",
        "workflows": "2000-01-01T00:00:00Z"
    }
    ```

5. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-helpscout --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

6. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json >> state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json | target-json >> state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run >> state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

7. Test the Tap
    
    While developing the HelpScout tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_helpscout -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 10.00/10 (previous run: 9.75/10, +0.25)
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json | singer-check-tap >> state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 135 messages for 8 streams.

        16 schema messages
        103 record messages
        16 state messages

    Details by stream:
    +----------------------+---------+---------+
    | stream               | records | schemas |
    +----------------------+---------+---------+
    | conversation_threads | 15      | 7       |
    | conversations        | 7       | 1       |
    | mailbox_fields       | 5       | 2       |
    | mailboxes            | 2       | 1       |
    | workflows            | 6       | 1       |
    | mailbox_folders      | 17      | 2       |
    | customers            | 49      | 1       |
    | users                | 2       | 1       |
    +----------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
