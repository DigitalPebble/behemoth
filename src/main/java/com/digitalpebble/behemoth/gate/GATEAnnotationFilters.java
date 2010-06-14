package com.digitalpebble.behemoth.gate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.digitalpebble.behemoth.Annotation;

/** Container for the annotation filters which is built from **/

public class GATEAnnotationFilters {
  
  HashSet<String> types;
  Map<String,Set<String>> featfilts;
  String annotationSetName;
  
  public static GATEAnnotationFilters getFilters(Configuration config) {
    GATEAnnotationFilters filter = new GATEAnnotationFilters();
    
    filter.annotationSetName = config.get("gate.annotationset.output", "");
    String[] stypes = config.get("gate.annotations.filter", "").split(",");
    String[] sFeatFilt = config.get("gate.features.filter", "").split(",");
    
    // the featurefilters have the following form : Type.featureName
    filter.featfilts = new HashMap<String,Set<String>>();
    for (String ff : sFeatFilt) {
      String[] fp = ff.split("\\.");
      if (fp.length != 2) continue;
      Set<String> fnames = filter.featfilts.get(fp[0]);
      if (fnames == null) {
        fnames = new HashSet<String>();
        filter.featfilts.put(fp[0], fnames);
      }
      fnames.add(fp[1]);
    }
    
    filter.types = new HashSet<String>();
    for (String s : stypes) {
      filter.types.add(s);
    }
    
    return filter;
  }
  
  public HashSet<String> getTypes() {
    return types;
  }
  
  public Map<String,Set<String>> getFeatfilts() {
    return featfilts;
  }
  
  public String getAnnotationSetName() {
    return annotationSetName;
  }
  
  public void setTypes(HashSet<String> types) {
    this.types = types;
  }
  
  public void setFeatfilts(Map<String,Set<String>> featfilts) {
    this.featfilts = featfilts;
  }
  
  public void setAnnotationSetName(String annotationSetName) {
    this.annotationSetName = annotationSetName;
  }
  
  /**
   * Returns a unmodifiable sorted list of all known annotations and feature
   * names so that we can use their position in the serialisation instead of
   * writing them as strings
   ***/
  public List<String> getLexicon() {
    HashSet<String> lexicon = new HashSet<String>();
    lexicon.addAll(types);
    Iterator<Set<String>> iter = featfilts.values().iterator();
    while (iter.hasNext()) {
      lexicon.addAll(iter.next());
    }
    ArrayList<String> temp = new ArrayList<String>(lexicon);
    Collections.sort(temp);
    return Collections.unmodifiableList(temp);
  }
  
}
