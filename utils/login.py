
import time
import json
import random
import toml
from seleniumwire import webdriver
from selenium.webdriver.common.by import By


def random_sleep_up_to_5s():
    sleep_time_ms = random.uniform(1000, 5000)
    sleep_time_s = sleep_time_ms / 1000
    time.sleep(sleep_time_s)


def get_auth() -> str:
    options = {
        'disable_encoding': True
    }

    with open('config.toml', 'r') as f:
        data = toml.load(f)

    usr = data['login']['username']
    pwd = data['login']['password']

    driver = webdriver.Chrome(seleniumwire_options=options)
    driver.get("https://echotik.live/login")
    driver.find_elements

    username_input = driver.find_element(By.ID, 'email_input')
    username_input.send_keys(usr)

    random_sleep_up_to_5s()
    password_input = driver.find_element(By.ID, 'password_input')
    password_input.send_keys(pwd)

    random_sleep_up_to_5s()
    login_button = driver.find_element(By.CLASS_NAME, "arco-btn-primary")
    login_button.click()

    random_sleep_up_to_5s()  # must sleep

    for r in driver.requests:
        if r.response:
            if r.url != "https://echotik.live/api/v1/users/login":
                continue

            token = r.response.body
            auth_body_json = json.loads(token.decode('utf-8'))
            return "Bearer " + auth_body_json['data']['access_token']


if __name__ == '__main__':
    pass
