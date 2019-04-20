package com.rollingstone.rabbitmq.consumer;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import com.rollingstone.config.ApplicationConstant;
import com.rollingstone.model.AccountDTO;

@Service
public class AccountRabbitMQMessageConsumerListener {

	private static final Logger log = LoggerFactory.getLogger(AccountRabbitMQMessageConsumerListener.class);
	
	@Autowired
	private JavaMailSender sender;
	
	@Value("${spring.mail.username}")
	String emailReceiver;
	
	@RabbitListener(queues = "${account.queue.name}")
	public void receivedMessageForApi(final AccountDTO accountDTO) {
		
		log.info("Received AccountDTO message "+ accountDTO.toString());
		
		try{
			sendEmail(accountDTO);
		}
		catch(HttpClientErrorException ex) {
			if (ex.getStatusCode() == HttpStatus.NOT_FOUND) {
				log.info("Error Happend Sending Email");
				try {
					Thread.sleep(ApplicationConstant.MESSAGE_RETRY_DELAY);
				}
				catch(InterruptedException ie) {
					
				}
				log.info("Throwing Exception so that message will be requeued in the queue" );
				throw new RuntimeException();
			}
			else {
				throw new AmqpRejectAndDontRequeueException(ex);
			}
		}
		
		log.info("Email Sent");
	}
	
	public String sendEmail(AccountDTO accountDTO) {
		MimeMessage message = sender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(message);
		
		try {
			log.info("Email Receiver :"+emailReceiver);
			helper.setTo(emailReceiver);
			helper.setText("Dear "+ accountDTO.getAccountName() + " Your Account has been created" +
			"With the AccountNumber :"+ accountDTO.getAccountNumber() + ")");
			helper.setSubject("Account Created");
		}
		catch(MessagingException me) {
			log.info("Error Sending Email :"+ me.getLocalizedMessage());
			return "Error Sending Email";
		}
		sender.send(message);
		return "Email Sent Successfully";
	}
}
