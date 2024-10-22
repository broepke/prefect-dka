"""Functions for Interacting with Slack"""

import asyncio
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_incoming_webhook_message


async def send_message(slack_webhook, text_only_message, message_block):
    await send_incoming_webhook_message(
        slack_webhook=slack_webhook,
        text=text_only_message,
        slack_blocks=message_block,
    )


def bad_wiki_page(person, wiki_page, emoji):
    """Deadpool Slack notifcation

    Args:
        person (str): Name of the Person
        emoji (str): Slack formatted emjo e.g., :bat:

    Returns:
        _type_: _description_
    """
    slack_webhook = SlackWebhook.load("slack-notifications")

    death_details = f"• Person: {person} \n• Wiki Page: {wiki_page}"

    text_only_message = f"{person} has a bad Wiki page identifier"

    message_block = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": emoji + " Bad Wiki Page Alert for: " + person + " " + emoji,  # noqa: E501
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": death_details}},
        {"type": "divider"},
    ]

    asyncio.run(
        send_message(slack_webhook, text_only_message, message_block)
    )  # Run the async function


def death_notification(person, birth_date, death_date, age, emoji):
    """Deadpool Slack notifcation

    Args:
        person (str): Name of the Person
        birth_date (str): Birth Date
        death_date (str): Death Date
        age (str): Age
        emoji (str): Slack formatted emjo e.g., :bat:

    Returns:
        _type_: _description_
    """
    slack_webhook = SlackWebhook.load("slack-notifications")

    death_details = (
        f"• Birth Date: {birth_date} \n• Death Date: {death_date} \n• Age: {age}"  # noqa: E501
    )

    text_only_message = f"{person} has died at the age of {age}"

    message_block = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": emoji + " New Death Alert For: " + person + " " + emoji,  # noqa: E501
            },
        },
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": death_details}},
    ]

    asyncio.run(
        send_message(slack_webhook, text_only_message, message_block)
    )  # Run the async function


def slack_notification(df, column_name, source_scraper, emoji):
    """General function to post a message to slack using the
       "Prefect" slack app

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
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": new_additions},
            },  # noqa: E501
        ]

        result = send_incoming_webhook_message(
            slack_webhook=slack_webhook,
            text=text_only_message,
            slack_blocks=message_block,
        )
    else:
        result = "No new companies."

    return result
