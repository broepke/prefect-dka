import os
from twilio.rest import Client
from prefect.blocks.notifications import TwilioSMS


def send_sms_via_prefect(message):
    twilio_webhook_block = TwilioSMS.load("twilio-dka", validate=False)
    twilio_webhook_block.notify(message)


def send_sms_via_api(message_text):
    account_sid = os.environ.get("TWILIO_SID")
    auth_token = os.environ.get("TWILIO_TOKEN")

    client = Client(account_sid, auth_token)

    message = client.messages.create(
        from_="+18449891781", body=message_text, to="+18777804236"
    )

    print(message.sid)
