package dhc.osgi.tftp.server.miniohelper.serializedxmlhelpers;

import java.util.List;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;


/**
 * Handles XML Response
 * @author Corbin Sumner {@literal <csumner@pavlovmedia.com>}
 *
 */
@Root(name="ListBucketResult")
public class ListBucketResult {
    @Element(name="Name", required=false)
    public String name;
    
    @Element(name="Prefix", required=false)
    public String prefix;
    
    @Element(name="NextContinuationToken", required=false)
    public String nextContinuationToken;
    
    @Element(name="KeyCount", required=false)
    public int keyCount;
    
    @Element(name="MaxKeys", required=false)
    public int maxKeys;
    
    @Element(name="Delimiter", required=false)
    public String delimiter;
    
    @Element(name="IsTruncated", required=false)
    public String isTruncated;
    
    @ElementList(entry="Contents", inline=true, required=false)
    public List<Content> contents;
}