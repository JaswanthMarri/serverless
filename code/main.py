import functions_framework
import os
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import sqlalchemy
import json
import base64
import uuid
import boto3
import logging
from botocore.exceptions import ClientError, WaiterError
from datetime import datetime, timedelta
from sqlalchemy import update
from sqlalchemy import MetaData, Table


logger = logging.getLogger(__name__)

@functions_framework.http
def hello_http(request):

    # get the request body and parse it
    request_json = request.get_json(silent=True)
    message = base64.b64decode(request_json['data']['data']).decode('utf-8')
    data = json.loads(message)
    logger.info(data)

    # generate a random token and set exp time
    random_token = generate_random_token()
    current_time = datetime.utcnow()
    tkn_exp_time = current_time + timedelta(minutes=2)
    generated_link = 'https://jaswanthmarri.com:8080/v1/user/register?token='+random_token


    # Create a update statement
    db = connect_with_connector()
    metadata = MetaData()
    table_name = 'useraccount'
    table = Table(table_name, metadata, autoload_with=db)
    update_stmt = table.update().values(token=random_token, token_exp_time=tkn_exp_time, link=generated_link).where(table.c.username == data['user_email'])


    with db.connect() as conn:
        print(conn.execute(
                sqlalchemy.text(
                    "SELECT * from useraccount LIMIT 5"
                )
            ).fetchall()) 
        conn.execute(update_stmt)
        conn.commit()
        print(conn.execute(
                sqlalchemy.text(
                    "SELECT * from useraccount LIMIT 5"
                )
            ).fetchall())

    # send Email to User through SES
    usage_demo(data['user_email'],random_token)
    name = 'World'
    return 'Hello {}!'.format(name)


#Connection establisment with postgres instance
def connect_with_connector() -> sqlalchemy.engine.base.Engine:

    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"] #"cloud-nw-dev:us-east1:mycloudsql-instance-webapp"  # e.g. '127.0.0.1' ('172.17.0.1' if deployed to GAE Flex)
    db_user =  os.environ["DB_USER"] # "HfKjwAfjPWVGXTXt"  # e.g. 'my-db-user'
    db_pass =  os.environ["DB_PASS"] #"iwXPN2NVMifJ1Boy"  # e.g. 'my-db-password'
    db_name =  os.environ["DB_NAME"]  #"test_db"  # e.g. 'my-database'
    db_port = "5432"  # e.g. 5432


    ip_type = IPTypes.PRIVATE

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
            port=db_port
        )
        return conn

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        # [START_EXCLUDE]
        # Pool size is the maximum number of permanent connections to keep.
        pool_size=5,
        # Temporarily exceeds the set pool_size if no connections are available.
        max_overflow=2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        pool_timeout=30,  # 30 seconds
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # re-established
        pool_recycle=1800,  # 30 minutes
        # [END_EXCLUDE]
    )
    return pool


def generate_random_token():
    return str(uuid.uuid4())


class SesDestination:
    """Contains data about an email destination."""

    def __init__(self, tos, ccs=None, bccs=None):
        """
        :param tos: The list of recipients on the 'To:' line.
        :param ccs: The list of recipients on the 'CC:' line.
        :param bccs: The list of recipients on the 'BCC:' line.
        """
        self.tos = tos
        self.ccs = ccs
        self.bccs = bccs

    def to_service_format(self):
        """
        :return: The destination data in the format expected by Amazon SES.
        """
        svc_format = {"ToAddresses": self.tos}
        if self.ccs is not None:
            svc_format["CcAddresses"] = self.ccs
        if self.bccs is not None:
            svc_format["BccAddresses"] = self.bccs
        return svc_format


# snippet-end:[python.example_code.ses.SesDestination]


# snippet-start:[python.example_code.ses.SesMailSender]
class SesMailSender:
    """Encapsulates functions to send emails with Amazon SES."""

    def __init__(self, ses_client):
        """
        :param ses_client: A Boto3 Amazon SES client.
        """
        self.ses_client = ses_client

    # snippet-end:[python.example_code.ses.SesMailSender]

    # snippet-start:[python.example_code.ses.SendEmail]
    def send_email(self, source, destination, subject, text, html, reply_tos=None):
        """
        Sends an email.

        Note: If your account is in the Amazon SES  sandbox, the source and
        destination email accounts must both be verified.

        :param source: The source email account.
        :param destination: The destination email account.
        :param subject: The subject of the email.
        :param text: The plain text version of the body of the email.
        :param html: The HTML version of the body of the email.
        :param reply_tos: Email accounts that will receive a reply if the recipient
                          replies to the message.
        :return: The ID of the message, assigned by Amazon SES.
        """
        send_args = {
            "Source": source,
            "Destination": destination.to_service_format(),
            "Message": {
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": text}, "Html": {"Data": html}},
            },
        }
        if reply_tos is not None:
            send_args["ReplyToAddresses"] = reply_tos
        try:
            response = self.ses_client.send_email(**send_args)
            message_id = response["MessageId"]
            logger.info(
                "Sent mail %s from %s to %s.", message_id, source, destination.tos
            )
        except ClientError:
            logger.exception(
                "Couldn't send mail from %s to %s.", source, destination.tos
            )
            raise
        else:
            return message_id



def usage_demo(email_id,tkn):
    print("-" * 88)
    print("Welcome to the Amazon Simple Email Service (Amazon SES) email demo!")
    print("-" * 88)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    ses_client = boto3.client('ses',
        region_name=  os.environ["AWS_REGION"],
        aws_access_key_id=  os.environ["AWS_ACCESS_KEY"], 
        aws_secret_access_key= os.environ["AWS_SECRET"]
    )
    ses_mail_sender = SesMailSender(ses_client)
    email = email_id

    # Include the URL in the email message
    test_message_text = f"Hello from the Amazon SES mail demo! Click here to register: https://jaswanthmarri.com:8080/v1/register?token={tkn}"
    test_message_html = "<p>Hello!</p><p>From the <b>Amazon SES</b> mail demo!</p><p>Click <a href='https://jaswanthmarri.com:8080/v1/register?token=" + tkn + "'>here</a> to register.</p>"


    print(f"Sending mail from {email} to {email}.")
    ses_mail_sender.send_email(
        'support@jaswanthmarri.com',
        SesDestination([email]),
        "Amazon SES demo",
        test_message_text,
        test_message_html,
    )
