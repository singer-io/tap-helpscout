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
    > pip install backoff
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

    ```bash
    tap-helpscout --config config.json --discover > catalog.json
    ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

6. Run the Tap in Sync Mode

    ```bash
    tap-helpscout --config config.json --catalog catalog.json
    ```
    OR
    ```bash
    tap-helpscout --config config.json | target-json
    ```

---

Copyright &copy; 2019 Stitch
