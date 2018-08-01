package handlers;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.net.URLEncoder;
import java.util.Properties;

public class ReportHandler {

	public static void report(String subject, String text) {
		try {
			String report_reciever_email = ConfigHandler.REPORT_RECEIVER_EMAIL;
			String url = "http://10.129.149.9:8080/meta/mail/?to=" + URLEncoder.encode(report_reciever_email) + "&body=" + URLEncoder.encode(text) + "&subject=" + URLEncoder.encode(subject);
			UtilsHandler.makeGetRequest(url);
//			LogHandler.logInfo("[Report]Report Sent=> Subject: "+subject);
		} catch (Exception e) {
			e.printStackTrace();
//			LogHandler.logInfo("[Report][ReportSendingError]"+e.getMessage());
		}
	}

	public static void reportError(String text) {
		String scriptIdentityText = ConfigHandler.SCRIPT_IDENTITY_TEXT;
		if(ConfigHandler.REPORT_ERROR) {
			ReportHandler.report(scriptIdentityText, "[Error]" + text);
		}
	}

	public static void reportInfo(String text) {
		String scriptIdentityText = ConfigHandler.SCRIPT_IDENTITY_TEXT;
		ReportHandler.report(scriptIdentityText, "[Info]" + text);
	}

}
