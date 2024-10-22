from prefect import flow
from utilities.util_slack import death_notification

@flow(name="Send Test Slack Message", log_prints=True)
def send_test_slack():
    person = "Captain Jack Sparrow"
    birth_date = "10-10-1545"
    death_date = "10-10-1765"
    age = 220
    emoji = ":pirate_flag:"


    death_notification(person, birth_date, death_date, age, emoji)

if __name__ == "__main__":
    send_test_slack()