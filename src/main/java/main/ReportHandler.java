package main;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class ReportHandler {

	public static void report(String subject, String text) {
		try {
			String report_reciever_email = ConfigHandler.REPORT_RECEIVER_EMAIL;
			final String report_sender_email = "seil@cse.iitb.ac.in";
			//Get the session object
			Properties props = new Properties();
			props.put("mail.smtp.host", "imap.cse.iitb.ac.in");
			props.put("mail.smtp.auth", "true");

			Session session = Session.getDefaultInstance(props,
					new javax.mail.Authenticator() {
						protected PasswordAuthentication getPasswordAuthentication() {
							return new PasswordAuthentication(report_sender_email, "seilers");
						}
					});
			//Compose the message
			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(report_reciever_email));
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(report_reciever_email));
			message.setSubject(subject);
			message.setText("[" + UtilsHandler.current_timestamp() + "]" + text);
			Transport.send(message);
		} catch (Exception e) {
			e.printStackTrace();
			LogHandler.logInfo("[ReportEmailError]" + e.getMessage());
		}
	}

	public static void reportError(String text) {
		String scriptIdentityText = ConfigHandler.SCRIPT_IDENTITY_TEXT;
		ReportHandler.report(scriptIdentityText, "[Error]" + text);
	}

	public static void reportInfo(String text) {
		String scriptIdentityText = ConfigHandler.SCRIPT_IDENTITY_TEXT;
		ReportHandler.report(scriptIdentityText, "[Info]" + text);
	}

}
