package gpath.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;

public class JarUtil {
    
	private static org.apache.commons.logging.Log Log = LogFactory.getLog(JarUtil.class);
	
    public static void SetJobJar(Job job, Class<?> jobClass){
        URL u = ClassLoader.getSystemResource(toPath(jobClass.getName()));
        if(u == null){
        	Log.info("I cannot get class loader's URL. Assume this class is loaded in Jar");
        	job.setJarByClass(jobClass);
        	return;
        }
        Log.info("ClassLoader URL: "+u);
        if(u.getProtocol().equalsIgnoreCase("jar")){
        	Log.info("Loaded from jar file, set it directly.");
            job.setJarByClass(jobClass);
        }else if(u.getProtocol().equalsIgnoreCase("file")){
            String base = System.getProperty("java.io.tmpdir");
            if(!base.endsWith(File.separator)){
                base += File.separator;
            }
            File classfile=new File(u.getFile());
            int hash = Math.abs(new Long(classfile.lastModified()).hashCode()+jobClass.hashCode());
            String jarname= base+hash+".jar";
            File jarfile = new File(jarname);
            if(jarfile.exists()){
                if(!jarfile.isFile()){
                    jarfile.delete();
                    createJar(jobClass, u, jarname);
                }else{
                	Log.info("Class loaded by .class, but jar file is already packed:" + jarname);
                }
            }else{
                createJar(jobClass, u, jarname);
            }
            try {
                job.getConfiguration().set("mapred.jar", jarfile.toURI().toURL().toString());
            } catch (MalformedURLException e) {
                job.getConfiguration().set("mapred.jar", jarfile.toString());
            }            
        }
    }

    private static void createJar(Class<?> jobClass, URL u, String jarname) {
    	Log.info("Class loaded by .class, trying to pack it.");
        String[] paths = u.getFile().split("\\/");
        String[] packs=jobClass.getName().split("\\.");
        int pathpos = paths.length - 2;
        int packpos = packs.length - 2;
        boolean mismatch = false;
        while(packpos>=0&&pathpos>=0){
            if(paths[pathpos].equals(packs[packpos])){
                pathpos--;
                packpos--;
            }else{
                mismatch = true;
                break;
            }
        }
        if(!mismatch){
            String path="";
            for(int i=0;i<=pathpos;i++){
                path+=paths[i]+File.separator;
            }
            Log.info("jar root: "+path+", creating "+jarname);
            
            JarOutputStream out;
            try {
                out = new JarOutputStream(new FileOutputStream(jarname));
                int total = writeJar(new File(path),new File(path),out);
                out.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
                PrintWriter writer=new PrintWriter(out);
                writer.println("Manifest-Version: 1.0");
                out.closeEntry();
                Log.info((total+1) +" files packed.");
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    private static int writeJar(File relative, File root, JarOutputStream out) throws IOException{
        if(root.isDirectory()){
            int total = 0;
            for(File f:root.listFiles()){
                total+=writeJar(relative, f, out);
            }
            return total;
        }else if(root.isFile()){
            if(root.canRead()){
                out.putNextEntry(new ZipEntry(relative(relative, root)));
                BufferedInputStream inputStream=new BufferedInputStream(new FileInputStream(root));
                byte[] buffer=new byte[512];
                int len = inputStream.read(buffer);
                while(len>0){
                    out.write(buffer, 0, len);
                    len = inputStream.read(buffer);
                }
                inputStream.close();
                out.closeEntry();
                return 1;
            }else{
                System.err.println("Warning: Cannot read "+root+", User classes may not be found.");
                return 0;
            }
        }else{
            return 0;
        }
    }
    
    private static String relative(File root,File file){
        String sr = root.getAbsolutePath();
        String sf = file.getAbsolutePath();
        if(sf.startsWith(sr)){
            sf = sf.substring(sr.length());
            if(sf.startsWith(File.separator)){
                sf = sf.substring(File.separator.length());
            }
        }
        return sf;
    }
    
    private static String toPath(String className){
        StringBuffer sb = new StringBuffer(className);
        for (int i=0; i < sb.length(); i++) {
          if (sb.charAt(i) == '.') {
            sb.setCharAt(i, '/');
          }
        }
        sb.append(".class");
        return sb.toString();
    }
    
}
