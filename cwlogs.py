import boto3
from datetime import datetime


class cwlogger:
    def __init__(self,
                 log_group_name,
                 log_stream_name,
                 region="ap-northeast-1",
                 session=None,
                 force_new_log_stream=False):
        self.log_stream_name = log_stream_name
        self.log_group_name = log_group_name
        self.region = region
        self.token = None

        # get logs client
        self.client = cwlogger.get_client(session=session)

        # get existing logstream
        streams = self.client.describe_log_streams(
            logGroupName=self.log_group_name)
        existing = False
        for stream in streams["logStreams"]:
            if stream["logStreamName"] == self.log_stream_name:
                existing = True
                self.stream = stream
                if "uploadSequenceToken" in stream:
                    self.token = stream['uploadSequenceToken']
        if force_new_log_stream or not existing:
            self.client.create_log_stream(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name)

    def log(self, message):
        if self.token is None:
            result = self.client.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name,
                logEvents=[{
                    "message": message,
                    "timestamp": int(datetime.now().strftime("%s%f")[:-3])
                }]
            )
        else:
            result = self.client.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name,
                logEvents=[{
                    "message": message,
                    "timestamp": int(datetime.now().strftime("%s%f")[:-3])
                }],
                sequenceToken=self.token
            )
        self.token = result["nextSequenceToken"]

    @classmethod
    def get_client(cls, session=None):
        if session is not None:
            if not isinstance(session, boto3.session.Session):
                error_str = "'session' argument must be {}".format(
                    boto3.session.Session)
                raise TypeError(error_str)
            return session.client("logs")
        else:
            return boto3.client("logs")

    @classmethod
    def create_log_group(cls, log_group_name, region="ap-northeast-1",
                         session=None):
        client = cwlogger.get_client(session=session)
        client.create_log_group(logGroupName=log_group_name)
