package com.jortage.s3migrate;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class S3Migrate {

	private static final ImmutableMap<String, String> shorthand = ImmutableMap.<String, String>builder()
			.put("aws", "https://s3.amazonaws.com")
			.put("do-sfo2", "https://sfo2.digitaloceanspaces.com")
			.put("do-nyc3", "https://nyc3.digitaloceanspaces.com")
			.put("do-sgp1", "https://sgp1.digitaloceanspaces.com")
			.put("do-fra1", "https://fra1.digitaloceanspaces.com")
			.put("do-ams3", "https://ams3.digitaloceanspaces.com")
			.put("wasabi-east", "https://s3.wasabisys.com")
			.put("wasabi-west", "https://s3.us-west-1.wasabisys.com")
			.put("jortage-pool", "https://pool-api.jortage.com")
			.build();

	public static void main(String[] args) {
		System.out.println("Welcome to s3migrate");
		Scanner scanner = new Scanner(System.in, "UTF-8");
		String fromServer;
		String toServer;
		String fromBucket;
		String toBucket;
		while (true) {
			fromServer = queryEndpoint(scanner, "What S3 server are you migrating from?");
			System.out.println("< Using "+fromServer+" as the source endpoint");
			fromBucket = query(scanner, "? What's the name of the bucket you're migrating from?");

			toServer = queryEndpoint(scanner, "What S3 server are you migrating to?");
			System.out.println("< Using "+toServer+" as the destination endpoint");
			toBucket = query(scanner, "? What's the name of the bucket you're migrating to?");

			System.out.println("! Going to migrate...");
			System.out.println("    from: "+fromServer+"/"+fromBucket);
			System.out.println("      to: "+toServer+"/"+toBucket);
			if (queryYN(scanner, "? Does this look right?")) {
				break;
			}
		}
		Gson gson = new Gson();
		File home = new File(System.getProperty("user.home"));
		File credentialsFile = new File(home, ".config/s3migrate/credentials.json");
		JsonObject credentials;
		if (credentialsFile.exists()) {
			try {
				credentials = gson.fromJson(Files.toString(credentialsFile, Charsets.UTF_8), JsonObject.class);
			} catch (Exception e) {
				credentials = new JsonObject();
				e.printStackTrace(System.out);
				System.out.println("! Failed to load saved credentials; continuing without them");
			}
		} else {
			credentials = new JsonObject();
		}
		String fromId = fromServer+"/"+fromBucket;
		String fromAccessId = null;
		String fromAccessKey = null;
		if (credentials.has(fromId)) {
			if (queryYN(scanner, "? Use saved credentials for source server and bucket?")) {
				JsonArray arr = credentials.get(fromId).getAsJsonArray();
				fromAccessId = arr.get(0).getAsString();
				fromAccessKey = arr.get(1).getAsString();
			}
		} else if (credentials.has(fromServer)) {
			if (queryYN(scanner, "? Use saved credentials for source server?")) {
				JsonArray arr = credentials.get(fromServer).getAsJsonArray();
				fromAccessId = arr.get(0).getAsString();
				fromAccessKey = arr.get(1).getAsString();
			}
		}
		if (fromAccessId == null) {
			fromAccessId = query(scanner, "? What's the Access ID for the source server?");
			fromAccessKey = query(scanner, "? What's the Access Key for the source server?");
			if (queryYN(scanner, "? Would you like to save these credentials for later?")) {
				JsonArray arr = new JsonArray();
				arr.add(fromAccessId);
				arr.add(fromAccessKey);
				credentials.add(fromId, arr);
				credentials.add(fromServer, arr);
				trySaveCredentials(gson, credentials, credentialsFile);
			}
		} else {
			System.out.println("< Using access key ID "+redact(fromAccessId)+" for source server.");
		}
		String toId = toServer+"/"+toBucket;
		String toAccessId = null;
		String toAccessKey = null;
		if (credentials.has(toId)) {
			if (queryYN(scanner, "? Use saved credentials for destination server and bucket?")) {
				JsonArray arr = credentials.get(toId).getAsJsonArray();
				toAccessId = arr.get(0).getAsString();
				toAccessKey = arr.get(1).getAsString();
			}
		} else if (credentials.has(toServer)) {
			if (queryYN(scanner, "? Use saved credentials for destination server?")) {
				JsonArray arr = credentials.get(toServer).getAsJsonArray();
				toAccessId = arr.get(0).getAsString();
				toAccessKey = arr.get(1).getAsString();
			}
		}
		if (toAccessId == null) {
			toAccessId = query(scanner, "? What's the Access ID for the destination server?");
			toAccessKey = query(scanner, "? What's the Access Key for the destination server?");
			if (queryYN(scanner, "? Would you like to save these credentials for later?")) {
				JsonArray arr = new JsonArray();
				arr.add(toAccessId);
				arr.add(toAccessKey);
				credentials.add(toId, arr);
				credentials.add(toServer, arr);
				trySaveCredentials(gson, credentials, credentialsFile);
			}
		} else {
			System.out.println("< Using access key ID "+redact(toAccessId)+" for destination server.");
		}
		if (queryYN(scanner, "! We're ready to migrate the bucket.\n"
				+ "  Any existing files at the same path in the new bucket as there are files in the old bucket WILL BE OVERWRITTEN!\n"
				+ "? This is your last chance: Perform the migration? This may incur large egress and ingress fees.")) {
			Properties overrides = new Properties();
			BlobStoreContext fromCtx = ContextBuilder.newBuilder(fromServer.endsWith("amazonaws.com") ? "aws-s3" : "s3")
					.endpoint(fromServer)
					.credentials(fromAccessId, fromAccessKey)
					.name("source")
					.overrides(overrides)
					.build(BlobStoreContext.class);
			BlobStoreContext toCtx = ContextBuilder.newBuilder(toServer.endsWith("amazonaws.com") ? "aws-s3" : "s3")
					.endpoint(toServer)
					.credentials(toAccessId, toAccessKey)
					.name("destination")
					.overrides(overrides)
					.build(BlobStoreContext.class);
			BlobStore from = fromCtx.getBlobStore();
			BlobStore to = toCtx.getBlobStore();
			startPrintProgress("Collecting files");
			PageSet<? extends StorageMetadata> allFromBlobs = from.list(fromBucket, new ListContainerOptions().recursive().maxResults(32));
			endPrintProgress();
			startPrintProgress("Copying files");
			updateProgress(0);
			AtomicLong done = new AtomicLong(0);
			final String fromBucketF = fromBucket;
			final String toBucketF = toBucket;
			while (true) {
				allFromBlobs.stream().parallel().forEach((sm) -> {
					long seconds = 5;
					while (true) {
						try {
							BlobAccess acc = from.getBlobAccess(fromBucketF, sm.getName());
							Blob fromBlob = from.getBlob(fromBucketF, sm.getName());
							Blob toBlob = to.blobBuilder(sm.getName())
								.tier(sm.getTier())
								.type(sm.getType())
								.userMetadata(sm.getUserMetadata())
								.payload(fromBlob.getPayload())
								.build();
							to.putBlob(toBucketF, toBlob, new PutOptions().setBlobAccess(acc));
							done.addAndGet(1);
							addProgress();
							break;
						} catch (Exception e) {
							e.printStackTrace(System.out);
							System.out.println("! Got an error. Trying "+sm.getName()+" again in "+seconds+" seconds...");
							try {
								Thread.sleep(seconds*1000);
							} catch (InterruptedException e1) {
							}
							if (seconds < 60) {
								seconds *= 2;
							}
						}
					}
				});
				if (allFromBlobs.getNextMarker() != null) {
					allFromBlobs = from.list(fromBucket, new ListContainerOptions().recursive().afterMarker(allFromBlobs.getNextMarker()).maxResults(32));
				} else {
					break;
				}
			}
			endPrintProgress();
			System.out.println("! All done.");
		} else {
			System.out.println("! Okay. Exiting.");
		}
	}

	private static Thread progressPrintThread = null;
	private static AtomicLong progressAmt = new AtomicLong(-1);

	private static void startPrintProgress(String str) {
		if (progressPrintThread != null) {
			endPrintProgress();
		}
		progressAmt.set(-1);
		String[] progress = { "|", "/", "-", "\\" };
		progressPrintThread = new Thread(() -> {
			try {
				int i = 0;
				while (true) {
					long amt = progressAmt.get();
					if (amt == -1) {
						System.out.print("\r"+progress[i++%4]+" "+str+"...");
					} else {
						System.out.print("\r"+progress[i++%4]+" "+str+" ("+amt+" done so far)");
					}
					Thread.sleep(50);
				}
			} catch (InterruptedException e) {
				long amt = progressAmt.get();
				System.out.print("\r  "+Strings.repeat(" ", str.length()+2+Long.toString(amt).length()+13)+"\r");
				return;
			}
		}, "Progress print thread");
		progressPrintThread.start();
	}

	private static void updateProgress(long amt) {
		progressAmt.set(amt);
	}

	private static void addProgress() {
		progressAmt.addAndGet(1);
	}

	private static void endPrintProgress() {
		if (progressPrintThread != null) {
			progressPrintThread.interrupt();
			try {
				progressPrintThread.join();
			} catch (InterruptedException e) {
			}
			progressPrintThread = null;
		}
	}

	private static String redact(String str) {
		if (str.length() <= 4) return Strings.repeat("*", str.length());
		return Strings.repeat("*", str.length()-4)+str.substring(str.length()-4, str.length());
	}

	private static void trySaveCredentials(Gson gson, JsonElement element, File file) {
		try {
			Files.createParentDirs(file);
			Files.write(gson.toJson(element), file, Charsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.out.println("! Failed to save credentials");
		}
	}

	private static String queryEndpoint(Scanner scanner, String query) {
		String endpoint;
		while (true) {
			endpoint = query(scanner, "? "+query+" (e.g. https://s3.amazonaws.com)\n"
					+ "  You may also answer a shorthand service name. The supported ones are "+Joiner.on(", ").join(shorthand.keySet()));
			if (shorthand.containsKey(endpoint.toLowerCase(Locale.ROOT))) {
				endpoint = shorthand.get(endpoint.toLowerCase(Locale.ROOT));
			}
			if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
				System.out.println("! That doesn't look like an S3 endpoint URL to me. It has to start with http:// or https://");
			} else {
				endpoint = endpoint.replaceFirst("/+$", "");
				break;
			}
		}
		return endpoint;
	}

	private static String query(Scanner scanner, String query) {
		System.out.println(query);
		while (true) {
			System.out.print("> ");
			String line = scanner.nextLine();
			if (line != null && !line.trim().isEmpty()) {
				return line;
			}
		}
	}

	private static boolean queryYN(Scanner scanner, String query) {
		boolean first = true;
		while (true) {
			String resp = query(scanner, first ? query+" (Y/N)" : "Please answer yes or no.").toLowerCase(Locale.ENGLISH);
			if ("y".equals(resp) || "yes".equals(resp)) {
				return true;
			} else if ("n".equals(resp) || "no".equals(resp)) {
				return false;
			}
			first = false;
		}
	}

	public enum YNA {
		YES, NO, ALWAYS
	}

	private static YNA queryYNA(Scanner scanner, String query) {
		boolean first = true;
		while (true) {
			String resp = query(scanner, first ? query+" (Y/N/A)" : "Please answer yes, no, or always.").toLowerCase(Locale.ENGLISH);
			if ("y".equals(resp) || "yes".equals(resp)) {
				return YNA.YES;
			} else if ("n".equals(resp) || "no".equals(resp)) {
				return YNA.NO;
			} else if ("a".equals(resp) || "always".equals(resp)) {
				return YNA.ALWAYS;
			}
			first = false;
		}
	}

}
