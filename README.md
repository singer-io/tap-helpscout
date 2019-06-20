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
3. Create your tap's `config.json` file which should look like the following:

    ```json
    {
        "client_id": "OAUTH_CLIENT_ID",
        "client_secret": "OAUTH_CLIENT_SECRET",
        "refresh_token": "YOUR_OAUTH_REFRESH_TOKEN",
        "start_date": "2017-04-19T13:37:30Z"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```
    {
        "currently_syncing": "users",
        "bookmarks": {
            "customers": "2019-06-11T13:37:55Z",
            "mailbox_folders": "2019-06-19T19:48:42Z",
            "mailboxes": "2019-06-18T18:23:58Z",
            "users": "2019-06-20T00:52:46Z",
            "workflows": "2019-06-19T19:48:44Z",
            "conversation_threads": "2019-06-11T13:37:55Z",
            "conversations": "2019-06-11T13:37:55Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-helpscout --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the HelpScout tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_helpscout -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 10.00/10 (previous run: 9.97/10, +0.03)
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-helpscout --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 150 messages for 8 streams.

        20 schema messages
        111 record messages
        19 state messages

    Details by stream:
    +----------------------+---------+---------+
    | stream               | records | schemas |
    +----------------------+---------+---------+
    | workflows            | 7       | 1       |
    | users                | 3       | 1       |
    | mailboxes            | 2       | 1       |
    | conversation_threads | 22      | 11      |
    | mailbox_folders      | 9       | 2       |
    | mailbox_fields       | 5       | 2       |
    | conversations        | 11      | 1       |
    | customers            | 52      | 1       |
    +----------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
