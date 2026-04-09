package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {


	public static BigInteger hashOf(String entity) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] hash = md.digest(entity.getBytes());

			return new BigInteger(1, hash);

		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	public static BigInteger addressSize() {
		return BigInteger.valueOf(2).pow(128);
	}

	public static int bitSize() {
		return 128;
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}
