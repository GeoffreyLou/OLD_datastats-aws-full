import os
import smtplib
import ssl

port = 465  
smtp_server = "smtp.gmail.com"
sender_email = os.environ['SENDER_EMAIL']
receiver_email = os.environ['RECEIVER_EMAIL']
password = os.environ['EMAIL_PASSWORD']


subject = "Test envoi mail"
message = f"Subject : Success\n\n\
    Le mail a été correctement envoyé"

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message.encode("utf-8"))