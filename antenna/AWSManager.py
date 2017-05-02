# Copyright 2016 Morgan McDermott & Blake Allen
import botocore
import boto3

class AWSManager(object):
    def __init__(self, aws_region="us-west-1", aws_profile=None,
                 aws_access_key_id=None, aws_secret_access_key=None):
        self._aws_region = aws_region
        self._aws_profile = aws_profile
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._session = None
        self.create_session()
        self.clients = {}

    def create_session(self):
        if self._session is None:
            try:
                self._session = boto3.Session(profile_name=self._aws_profile,
                                              region_name=self._aws_region
                )
            except botocore.exceptions.ProfileNotFound:
                self._session = boto3.Session(
                    region_name=self._aws_region,
                    aws_access_key_id=self._aws_access_key_id,
                    aws_secret_access_key=self._aws_secret_access_key
                )
            except Exception as e: # TODO: Determine specific exception
                self._session = boto3.Session(region_name=self._aws_region)

    def get_client(self, service):
        """
        Return a client configured with current credentials and region
        """
        if service not in self.clients:
            self.clients[service] = self._session.client(service)
        return self.clients[service]
