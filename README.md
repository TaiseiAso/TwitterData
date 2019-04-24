# TwitterData
***
Tool to collect multi-turn dialogues by two talker from Twitter in real time.

## Requirement
- python3
- yaml
- tweepy
- MeCab
- Twitter API key
    - consumer_key
    - consumer_secret
    - access_token
    - access_token_secret

## Usage
1. Get Twitter API key and save file "config/api.yml".
    ```
    twitter_API:
      consumer_key        : xxxxx
      consumer_secret     : xxxxx
      access_token        : xxxxx
      access_token_secret : xxxxx
    ```

2. Start Collecting tweets by twitter.py.
    ```
    $ python twitter.py
    ```

3. Push Ctrl+C to stop safely and save tweet data in "data/".

4. Filtering data by filter.py.
    ```
    $ pyton filter.py
    ```
    Please edit "config/config.yml" to change filter.

5. Delete all data by clear.py.
    ```
    $ python clear.py
    ```
