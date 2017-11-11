package com.github.bogdanovmn.downloadwlc;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.UrlResource;
import org.springframework.util.DigestUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UrlContentDiskCache {
	private static final Logger LOG = LoggerFactory.getLogger(UrlContentDiskCache.class);

	private static final String PROPERTY_BASE_DIR = "urlContentDiskCache.baseDir";

	private Path baseDir;
	private final Map<String, File> files = new HashMap<>();
	private final String tag;

	private boolean isInitialized = false;


	public UrlContentDiskCache(String tag) {
		this.tag = tag;
	}

	public UrlContentDiskCache(Class<?> clazz) {
		this.tag = clazz.getSimpleName();
	}

	private void init()
		throws IOException
	{
		if (!this.isInitialized) {
			LOG.info("Init cache...");

			String baseDirProperty = System.getProperty(PROPERTY_BASE_DIR, "");
			if (baseDirProperty.isEmpty()) {
				throw new IOException(
					String.format("Cache base dir expected (use %s property)", PROPERTY_BASE_DIR)
				);
			}
			this.baseDir = Paths.get(baseDirProperty, this.tag);

			if (!this.baseDir.toFile().exists()) {
				LOG.info("Create base dir: {}", this.baseDir);
				Files.createDirectories(this.baseDir);
			}

			try {
				Iterator<File> fileIterator = FileUtils.iterateFiles(this.baseDir.toFile(), null, true);
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
					this.files.put(nameParts[0], file);
				}
			}
			finally {
				this.isInitialized = true;
			}
			LOG.info("Init completed. Total urls in cache: {}", this.files.entrySet().size());
		}

	}

	private File put(URL url)
		throws IOException
	{
		Path newFilePath = Paths.get(
			this.baseDir.toString(),
			this.urlToFileName(url).toString()
		);

		File outputFile;
		if (newFilePath.getParent().toFile().exists() || newFilePath.getParent().toFile().mkdirs()) {
			outputFile = Files.createFile(newFilePath).toFile();

			try (
				FileOutputStream output = new FileOutputStream(outputFile)
			) {
				IOUtils.copy(
					new UrlResource(url).getInputStream(),
					output
				);
			}

			this.files.put(this.urlToKey(url), outputFile);
		}
		else {
			throw new IOException("Can't create cache dir: " + newFilePath.getParent().toString());
		}

		return outputFile;
	}

	private String urlToKey(URL url) {
		return DigestUtils.md5DigestAsHex(url.toString().getBytes());
	}

	private Path urlToFileName(URL url) {
		return Paths.get(
			url.getHost().replaceAll("\\W", "_"),
			String.format(
				"%s.%s",
				this.urlToKey(url),
				String.format("%s__%s", url.getPath(), url.getQuery())
					.replaceAll("\\W", "_")
			)
		);
	}

	private byte[] get(URL url)
		throws IOException
	{
		this.init();

		byte[] result = null;

		File file = this.files.get(this.urlToKey(url));
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
