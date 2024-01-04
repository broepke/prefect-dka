"""
General Twilio and SMS sending utilities
"""
from twilio.rest import Client
from prefect.blocks.notifications import TwilioSMS
from prefect.blocks.system import Secret
from utilities.util_apify import the_arbiter


def send_sms_via_prefect(message):
    twilio_webhook_block = TwilioSMS.load("twilio-dka", validate=False)
    twilio_webhook_block.to_phone_numbers = ["+14155479222"]
    twilio_webhook_block.notify(message)


def send_sms_via_api(message_text, distro_list, arbiter=False):
    account_sid_block = Secret.load("twilio-sid")
    auth_token_block = Secret.load("twilio-token")
    from_number_block = Secret.load("twilio-from")

    # Access the stored secret
    account_sid = account_sid_block.get()
    auth_token = auth_token_block.get()
    from_number = from_number_block.get()

    if arbiter:
        message_text = the_arbiter(message_text)

    client = Client(account_sid, auth_token)

    if distro_list == []:
        return None
    else:
        for number in distro_list:
            message = client.messages.create(
                from_=from_number, body=message_text, to=number
            )

    return message.sid
