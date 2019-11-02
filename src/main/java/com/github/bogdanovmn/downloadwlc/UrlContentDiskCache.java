package com.github.bogdanovmn.downloadwlc;

import com.github.bogdanovmn.httpclient.simple.SimpleHttpClient;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UrlContentDiskCache {
	private static final Logger LOG = LoggerFactory.getLogger(UrlContentDiskCache.class);

	private static final String PROPERTY_BASE_DIR = "urlContentDiskCache.baseDir";

	private Path baseDir;
	private final Map<String, File> files = new HashMap<>();
	private final String tag;

	private final SimpleHttpClient httpClient = new SimpleHttpClient();

	private boolean isInitialized = false;


	public UrlContentDiskCache(String tag) {
		this.tag = tag;
	}

	public UrlContentDiskCache(Class<?> clazz) {
		this.tag = clazz.getSimpleName();
	}

	private synchronized void init() throws IOException {
		if (!isInitialized) {
			LOG.info("Init cache...");

			String baseDirProperty = System.getProperty(PROPERTY_BASE_DIR, "");
			if (baseDirProperty.isEmpty()) {
				throw new IOException(
					String.format("Cache base dir expected (use %s property)", PROPERTY_BASE_DIR)
				);
			}
			baseDir = Paths.get(baseDirProperty, tag);

			if (!this.baseDir.toFile().exists()) {
				LOG.info("Create base dir: {}", baseDir);
				Files.createDirectories(baseDir);
			}

			try {
				Iterator<File> fileIterator = FileUtils.iterateFiles(baseDir.toFile(), null, true);
				while (fileIterator.hasNext()) {
					File file = fileIterator.next();
					String[] nameParts = file.getName().split("\\.", 2);
					if (nameParts.length != 2) {
						throw new RuntimeException(
							String.format(
								"Corrupted cache file: %s",
								file.toString()
							)
						);
					}
					files.put(nameParts[0], file);
				}
			}
			finally {
				isInitialized = true;
			}
			LOG.info("Init completed. Total urls in cache: {}", files.entrySet().size());
		}

	}

	private File put(URL url) throws IOException {
		Path newFilePath = Paths.get(
			baseDir.toString(),
			urlToFileName(url).toString()
		);

		File outputFile;
		if (newFilePath.getParent().toFile().exists() || newFilePath.getParent().toFile().mkdirs()) {
			outputFile = Files.createFile(newFilePath).toFile();

			try (
				FileOutputStream output = new FileOutputStream(outputFile)
			) {
				output.write(
					httpClient.get(url.toString())
						.getBytes()
				);
			}

			files.put(urlToKey(url), outputFile);
			LOG.info("Download to {}", newFilePath.toString());
		}
		else {
			throw new IOException("Can't create cache dir: " + newFilePath.getParent().toString());
		}

		return outputFile;
	}

	private String urlToKey(URL url) {
		try {
			return String.format(
				"%032x",
				new BigInteger(
					1,
					MessageDigest.getInstance("MD5")
						.digest(url.toString().getBytes())
				)
			);
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	private Path urlToFileName(URL url) {
		String key = urlToKey(url);
		String keyFirstChar = key.substring(0, 1);

		return Paths.get(
			url.getHost().replaceAll("\\W", "_"),
			keyFirstChar,
			String.format(
				"%s.%s",
				key,
				String.format("%s__%s", url.getPath(), url.getQuery())
					.replaceAll("\\W", "_")
					.replaceFirst("__null", "")
			)
		);
	}

	public byte[] get(URL url) throws IOException {
		this.init();

		byte[] result;

		File file = files.get(urlToKey(url));
		if (file == null) {
			LOG.info("Cache not found: {}", url.toString());
			file = this.put(url);
		}

		result = Files.readAllBytes(file.toPath());
		return result;
	}

	public boolean delete(URL url) {
		File file = this.files.remove(this.urlToKey(url));
		if (file != null) {
			try {
				Files.delete(file.toPath());
			}
			catch (IOException e) {
				return false;
			}
			return true;
		}
		return false;
	}

	public String getText(URL url)
		throws IOException
	{
		return new String(this.get(url), Charset.defaultCharset());
	}
}
