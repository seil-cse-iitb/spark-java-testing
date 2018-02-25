package main;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class ReportHandler {

    public static void report(String subject, String text) {
        try {
            String report_reciever_email = ConfigHandler.REPORT_RECEIVER_EMAIL;
            final String report_sender_email = "seil@cse.iitb.ac.in";
            Properties properties = System.getProperties();
            properties.setProperty("mail.smtp.host", "imap.cse.iitb.ac.in");
            properties.setProperty("mail.smtp.auth", "true");
            Authenticator auth = new Authenticator() {
	            //override the getPasswordAuthentication method
	            protected PasswordAuthentication getPasswordAuthentication() {
		            return new PasswordAuthentication(report_sender_email, "");
	            }
            };
            Session session = Session.getDefaultInstance(properties);
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(report_sender_email));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(report_reciever_email));
            message.setSubject(subject);
            message.setText("[" + UtilsHandler.current_timestamp() + "]" + text);
            Transport.send(message);
        } catch (MessagingException e){
            e.printStackTrace();
        }
    }

    public static void reportError(String text){
        String scriptIdentityText = ConfigHandler.SCRIPT_IDENTITY_TEXT;
        ReportHandler.report(scriptIdentityText, "[Error]" + text);
    }

    public static void reportInfo(String text){
        String scriptIdentityText = ConfigHandler.SCRIPT_IDENTITY_TEXT;
        ReportHandler.report(scriptIdentityText, "[Info]" + text);
    }

}
