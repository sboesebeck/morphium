package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by stephan on 25.09.14.
 */
public class EntityCache {

    //    private StringBuffer sb = new StringBuffer();
    private Logger log = Logger.getLogger(EntityCache.class);

    private volatile Map<String, Class> entityByTypeId = new Hashtable<>();
    private volatile Vector<Class> enumlist = new Vector<>();
    private volatile Hashtable<Class<?>, NameProvider> nameProviders = new Hashtable<>();

    private volatile AnnotationAndReflectionHelper hlp = new AnnotationAndReflectionHelper(true);
    private volatile NameProvider defaultNameProvider = new DefaultNameProvider();


    private List<String> paths;
    public EntityCache() {
        paths = new ArrayList<>();
        String classpath = System.getProperty("java.class.path");
        String[] p = classpath.split(System.getProperty("path.separator"));
        paths.addAll(Arrays.asList(p));

        String javaHome = System.getProperty("java.home");
        paths.add(javaHome + File.separator + "lib");

        updateClasses();
    }

    public void addClassPathElement(String el) {
        paths.add(el);

        traverseClassesInDir(new File(el), new File(el));
    }


    public Map<String, Class> getEntityByTypeId() {
        return entityByTypeId;
    }

    public Vector<Class> getEnumlist() {
        return enumlist;
    }

    public Hashtable<Class<?>, NameProvider> getNameProviders() {
        return nameProviders;
    }

    private void registerEntity(String id, Class cls) {
        if (entityByTypeId.get(id) != null) {
            entityByTypeId.put(cls.getName(), cls);
            log.warn("ID not unique - maybe inherited");
        } else {
            entityByTypeId.put(id, cls);
        }
    }

    private void gotClassName(String className) {
        if (className.startsWith("com.sun")) return;
        if (className.startsWith("sun.")) return;
        if (className.startsWith("java.")) return;
        if (className.startsWith("javax.")) return;
        if (className.startsWith("javafx.")) return;
        if (className.startsWith("org.apache.")) return;
        if (className.startsWith("org.junit.")) return;
        if (className.startsWith("net.sf.")) return;
        if (className.startsWith("com.javafx")) return;
        if (className.startsWith("com.apple")) return;
        if (className.startsWith("apple")) return;
        if (className.startsWith("com.oracle")) return;
        if (className.startsWith("jdk.internal.")) return;
        if (className.startsWith("netscape.")) return;
        if (className.startsWith("junit.")) return;

//            if (className.contains("swing")) return;

//                if (o.startsWith("de.caluga.test.mongo.suite.")) log.info("got it");
        try {
//                log.info("Testing "+className);
            Class cls = Class.forName(className);
            Entity ent = hlp.getAnnotationFromHierarchy(cls, Entity.class);
            Embedded emb = hlp.getAnnotationFromHierarchy(cls, Embedded.class);


            if (ent != null) {
                if (cls.isAnnotationPresent(Entity.class)) {

                    if (ent.typeId().equals(".")) {
                        registerEntity(className, cls);
                    } else {
                        registerEntity(ent.typeId(), cls);
                    }

                    if (ent.nameProvider() != null) {
                        try {
                            nameProviders.put(cls, ent.nameProvider().newInstance());
                        } catch (Exception e) {
                            log.fatal("Error instanciating Nameprovider: " + ent.nameProvider().getName() + " for type " + cls.getName(), e);
                        }
                    } else {
                        nameProviders.put(cls, defaultNameProvider);
                    }
                } else {
                    //inherited annotation
                    registerEntity(className, cls);
                }


            } else if (emb != null) {
                if (cls.isAnnotationPresent(Embedded.class)) {
                    if (emb.typeId().equals(".")) {
                        registerEntity(className, cls);
                    } else {
                        registerEntity(emb.typeId(), cls);
                    }
                } else {
                    //inherited
                    registerEntity(className, cls);
                }
            } else if (cls.isEnum()) {
                enumlist.add(cls);
            }

        } catch (Throwable e) {
            //swollowing exception
        }
    }

    public void updateClasses() {
        nameProviders.clear();
        entityByTypeId.clear();
        enumlist.clear();
        for (String path : paths) {
            File file = new File(path);
            if (file.exists()) {
                traverseClassesInDir(file, file);
            }
        }

    }

    private void traverseClassesInDir(File root, File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                traverseClassesInDir(root, child);
            }
        } else {
            if (file.getName().toLowerCase().endsWith(".jar")) {
                JarFile jar = null;
                try {
                    jar = new JarFile(file);
                } catch (Exception ex) {

                }
                if (jar != null) {
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        int extIndex = name.lastIndexOf(".class");
                        if (extIndex > 0) {
                            gotClassName(name.substring(0, extIndex).replace("/", "."));
                        }
                    }
                }
            } else if (file.getName().toLowerCase().endsWith(".class")) {
                StringBuffer sb = new StringBuffer();
                String fileName = file.getName();
                sb.append(fileName.substring(0, fileName.lastIndexOf(".class")));
                file = file.getParentFile();
                while (file != null && !file.equals(root)) {
                    sb.insert(0, '.').insert(0, file.getName());
                    file = file.getParentFile();
                }
                gotClassName(sb.toString());

            }
        }

    }

}
