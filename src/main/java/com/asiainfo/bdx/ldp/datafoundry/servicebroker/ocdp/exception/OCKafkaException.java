package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception;

public class OCKafkaException extends Exception{

	private static final long serialVersionUID = -6219728739447663913L;

	public OCKafkaException(String string) {
		super(string);
	}

	public OCKafkaException(String string, Throwable t)
	{
		super(string, t);
	}
}
