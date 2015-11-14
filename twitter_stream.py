import hashlib
import time
import sys, re
import simplejson as json
from twython import TwythonStreamer
from optparse import OptionParser
from states import states

# setup signal so cmd pipe can work
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)

length_limit = 10
candidate_list = ['donald trump', "hillary clinton", "bernie sanders", "martin o'malley", 'ted cruz', 'ben carson', 'rand paul', 'jeb bush', 'chris christie', 'carly fiorina', 'linsey graham', 'mike huckabee', 'bobby jindal', 'john kasich', 'marco rubio', 'rick santorum']

user_filter_dict = {}
window_length = 1200

def get_candidate_name(tweet):
    for candidate in candidate_list:
        candidate_name = candidate.split()
        match = 0
        tweet_text_lower = tweet['text'].lower()
        for item in candidate_name:
            if item in tweet_text_lower:
                match += 1

        if match == len(candidate_name):
            return candidate

    return None

def should_ignore_user(user_id):
    if user_id in user_filter_dict:
        user_timestamp = user_filter_dict[user_id]
        if user_timestamp > time.time():
            return True
        else:
            user_filter_dict[user_id] = time.time() + window_length
            return False
    else:
        user_filter_dict[user_id] = time.time() + window_length
        return False

def tweet_text_clean_up(tweet):
    tweet = re.sub(r"(?:\@|https?\://)\S+", "", tweet)
    return tweet


def is_valid_tweet(tweet):
    return len(tweet.split()) > length_limit


def print_err(text):
    sys.stderr.write(text + '\n')


class TwitterStream(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            candidate_name = get_candidate_name(data)
            if candidate_name:
                data['candidate_name'] = candidate_name
            else:
                return

            user_id = data['user']['id']

            if should_ignore_user(user_id):
                return

            ############################################
            print_err(str(data['geo']))

            ori_tweet = data['text'].encode('utf-8')
            tweet = tweet_text_clean_up(ori_tweet)
            state_location = 'none'
            if is_valid_tweet(tweet) and data['user']['lang']=='en':
                print_err('-' * 50 + '\n' + tweet + '\n')
                if data['user'].get('location'):
                    location = re.sub(r'[^A-Za-z\d ]','',str(data['user']['location'].encode('utf-8'))).lower()
                    print_err(location)
                    location = location.split(' ')

                    location = map(lambda x: x.lower().strip(), location)
                    print_err(str(location))
                    for state in states.keys():
                        vals = map(lambda x:x.lower().strip(),states[state])
                        if any(map(lambda v: v in vals, location)):
                           state_location = state
            data['state'] = state_location
            print_err(state_location)


            if data['user']['lang'] =='en':
                    print json.dumps(data)
            else:
                print_err(ori_tweet + '\n')

    def on_error(self, status_code, data):
        sys.stderr.write(str(status_code) + '\n')
        exit(1)

APP_KEY = ""
APP_SECRET = ""
OAUTH_TOKEN = ""
OAUTH_TOKEN_SECRET = ""

stream = TwitterStream(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option(
        "-t", "--track",
        dest="track",
        default="twitter")
    options, _ = parser.parse_args()

    stream.statuses.filter(track=",".join(candidate_list))
