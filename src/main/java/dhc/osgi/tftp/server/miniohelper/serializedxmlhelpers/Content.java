package dhc.osgi.tftp.server.miniohelper.serializedxmlhelpers;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/**
 * 
 * @author Corbin Sumner {@literal <csumner@pavlovmedia.com>}
 *
 */
@Root
public class Content {
    @Element(name="Key", required=false)
    public String key;
    
    @Element(name="LastModified", required=false)
    public String lastModified;
    
    @Element(name="ETag", required=false)
    public String eTag;
    
    @Element(name="Size", required=false)
    public String size;
    
    @Element(name="StorageClass", required=false)
    public String storageClass;
    
    public String toString() {
        return "\n key: " + key + "\n ETag: " + eTag + "\n size: " + size + "\n";
    }
}
