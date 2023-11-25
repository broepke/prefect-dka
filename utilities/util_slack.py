"""Functions for Interacting with Slack"""
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_incoming_webhook_message


def slack_notification(df, column_name, source_scraper, emoji):
    """General function to post a message to slack using the "Prefect" slack app

    Args:
        df (Dataframe): DF of all newly added companies
        column_name (String): Name of the Column with company names
        source_scraper (String): Descriptive name of site scraped
        emoji (String): Slack code for the emojo to decorate the post ":pig:"

    Returns:
        String: Completion message for logging
    """
    if len(df) > 0:
        slack_webhook = SlackWebhook.load("slack-notifications")
        new_additions = " \n ".join(["• " + name for name in df[column_name]])

        text_only_message = "New Companies Added to Rookies VFX"

        message_block = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": emoji
                    + " New Companies Added to "
                    + source_scraper
                    + " "
                    + emoji,
                },
            },
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": new_additions}},
        ]

        result = send_incoming_webhook_message(
            slack_webhook=slack_webhook,
            text=text_only_message,
            slack_blocks=message_block,
        )
    else:
        result = "No new companies."

    return result
