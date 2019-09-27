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

## Streams
[**conversations**](https://developer.helpscout.com/mailbox-api/endpoints/conversations/list/)
- Endpoint: https://api.helpscout.net/v2/conversations
- Primary keys: id
- Foreign keys: mailbox_id (mailboxes), assignee > id (users), created_by > id (users), primary_customer > id (customers), custom_fields > id (mailbox_fields)
- Replication strategy: Incremental (query all, filter results)
  - Filter: status = all
  - Sort by: modifiedAt ascending
  - Bookmark: user_updated_at
- Transformations: Fields camelCase to snake_case
- Children: conversation_threads

[**conversation_threads**](https://developer.helpscout.com/mailbox-api/endpoints/conversations/threads/list/)
- Endpoint: https://api.helpscout.net/v2/conversations/{conversation_id}/threads
- Primary keys: id
- Foreign keys: conversation_id (conversations), customer > id (customers), created_by > id (users), assigned_to > id (users)
- Replication strategy: Full table (ALL for each parent Conversation)
  - Bookmark: None
- Transformations: Fields camelCase to snake_case. De-nest attachments array node. Add parent conversation_id field.
- Parent: conversations

[**customers**](https://developer.helpscout.com/mailbox-api/endpoints/customers/list/)
- Endpoint: https://api.helpscout.net/v2/customers
- Primary keys: id
- Foreign keys: None
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: modifiedSince
  - Sort by: modifiedAt ascending
  - Bookmark: updated_at (date-time)
- Transformations: Fields camelCase to snake_case. De-nest the following nodes: address, chats, emails, phones, social_profiles, websites.

[**mailboxes**](https://developer.helpscout.com/mailbox-api/endpoints/mailboxes/list/)
- Endpoint: https://api.helpscout.net/v2/mailboxes
- Primary keys: id
- Foreign keys: None
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: Fields camelCase to snake_case.
- Children: mailbox_fields, mailbox_folders

[**mailbox_fields**](https://developer.helpscout.com/mailbox-api/endpoints/mailboxes/mailbox-fields/)
- Endpoint: https://api.helpscout.net/v2/mailboxes/{mailbox_id}/fields
- Primary keys: id
- Foreign keys: mailbox_id (mailboxes)
- Replication strategy: Full table (ALL for each parent Mailbox)
  - Bookmark: None
- Transformations: Fields camelCase to snake_case. Add parent mailbox_id field.
- Parent: mailboxes

[**mailbox_folders**](https://developer.helpscout.com/mailbox-api/endpoints/mailboxes/mailbox-folders/)
- Endpoint: https://api.helpscout.net/v2/mailboxes/{mailbox_id}/folders
- Primary keys: id
- Foreign keys: mailbox_id (mailboxes)
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: Fields camelCase to snake_case. Add parent mailbox_id field.
- Parent: mailboxes

[**users**](https://developer.helpscout.com/mailbox-api/endpoints/users/list/)
- Endpoint: https://api.helpscout.net/v2/users
- Primary keys: id
- Foreign keys: None
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: Fields camelCase to snake_case.

[**workflows**](https://developer.helpscout.com/mailbox-api/endpoints/workflows/list/)
- Endpoint: https://api.helpscout.net/v2/users
- Primary keys: id
- Foreign keys: mailbox_id (mailboxes)
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: modified_at (date-time)
- Transformations: Fields camelCase to snake_case.


## Authentication
[Refresh Access Token](https://developer.helpscout.com/mailbox-api/overview/authentication/#4-refresh-access-token)
The tap should provides a `refresh_token`, `client_id` and `client_secret` to get an `access_token` when the tap starts. If/when the access_token expires in the middle of a run, the tap gets a new `access_token` and `refresh_token`. The `refresh_token` expires every use and new one is generated and persisted in the tap `config.json` until the next authentication.
To generate the necessary API keys: `client_id` and `client_secret`, follow these instructions to [Create My App](https://developer.helpscout.com/mailbox-api/overview/authentication/#oauth2-application) in your User Profile of the HelpScout web console application.
- App Name: tap-helpscout
- Redirect URL: https://app.stitchdata.test:8080/v2/integrations/platform.helpscout/callback
Record your credentials (for the tap config.json):
- App ID: `client_id`
- App Secret: `client_secret`

Authentication URL: https://secure.helpscout.net/authentication/authorizeClientApplication?client_id=`YOUR_CLIENT_ID`&state=`YOUR_CLIENT_SECRET`
Adjust the above URL by replacing `YOUR_CLIENT_ID` and `YOUR_CLIENT_SECRET`. In your web browser, disable any ad/popup blockers, and navigate to the Authorize URL. Click Authorize and you will be redirected to your Redirect URL (from above). Record the `code` from the redirected browser URL in the browser header.

Authentication curl: Run the following curl command (from the command line or REST client) with the parameters replaced to return your access_token and refresh_token. This is a POST request.
```bash
> curl -0 -v -X POST https://api.helpscout.net/v2/oauth2/token
    -H "Accept: application/json"\
    -H "application/x-www-form-urlencoded"\
    -d "grant_type=authorization_code"\
    -d "code=YOUR_CODE"\
    -d "client_id=YOUR_CLIENT_ID"\
    -d "client_secret=YOUR_CLIENT_SECRET"
```
Record your `client_id`, `client_secret`, and the returned `refresh_token` into your tap `config.json`, which should look like the following:

    ```json
    {
        "client_id": "OAUTH_CLIENT_ID",
        "client_secret": "OAUTH_CLIENT_SECRET",
        "refresh_token": "YOUR_OAUTH_REFRESH_TOKEN",
        "start_date": "2017-04-19T13:37:30Z"
    }
    ```

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
