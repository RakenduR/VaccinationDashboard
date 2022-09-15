
import imaplib, email, os
import logging


def auth(user,password,imap_url):
    try:
        con = imaplib.IMAP4_SSL(imap_url)
        con.login(user,password)
    except Exception as e:
        print("Error while logging into email:" + str(e))
        logging.info("Error while logging into email:" + str(e))
        exit()
    return con



def download_attachment(email_sender,email_subject,outputDir,recipientAccount,emailPassword,emailUrl):
    msgs = []
    errors = []
    fileName = ""

    imap_url = emailUrl
    user = recipientAccount
    password = emailPassword

    download_folder = os.path.join(os.getcwd(), outputDir )
 #archive_folder = os.path.join(os.getcwd(), 'SyncEmails/Archives' )

    con = auth(user,password,imap_url)
    con.select('INBOX')

    # Retrieve emails with subject SG bookings within Blackout window
    _, message_numbers_raw = con.search(None,'(UNSEEN)' ,'FROM','"' + email_sender +'"','SUBJECT','"'+email_subject+'"')

    for message_number in message_numbers_raw[0].split():
        _, msg = con.fetch(message_number, '(RFC822)')
        msgs.append(msg)
        # Parse the raw email message in to a convenient object
        message = email.message_from_bytes(msg[0][1])
        
        # Iterate through the email and find necessary info
        for part in message.walk():
           
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            fileName = part.get_filename()

            if bool(fileName):
                filePath = os.path.join(download_folder, fileName)
                if not os.path.isfile(filePath) :
                    fp = open(filePath, 'wb')
                    fp.write(part.get_payload(decode=True))
                    fp.close()    
    return fileName