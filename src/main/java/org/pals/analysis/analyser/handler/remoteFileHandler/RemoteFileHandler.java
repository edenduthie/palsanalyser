package org.pals.analysis.analyser.handler.remoteFileHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

public interface RemoteFileHandler
{
	public abstract void copyRemoteFileToLocal(URL remoteFileUrl, File localFile) throws IOException;
	public abstract void storeFilesIntoStore(Map<String, File> localFiles, Map<String, URL> remoteUrls) throws IOException;
	public abstract URL getStoreUrl(URL remoteFileUrl);
}
