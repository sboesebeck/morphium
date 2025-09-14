package de.caluga.test;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class CollatorTests {
    private Logger log = LoggerFactory.getLogger(CollatorTests.class);
    @Test
    @Tag("core")
    public void collatorTests() throws Exception{
        log.info("This should work");
        RuleBasedCollator r = (RuleBasedCollator) Collator.getInstance(Locale.forLanguageTag("de"));

//        var rules="a<b<c<d<e<f<g<h<i<j<k<l<m<n<o<p<q<r<s<t<u<v<w<x<y<z";
//        rules=",A<B<C<D<E<F<G<H<I<J<K<L<M<N<O<P<Q<R<S<T<U<V<W<X<Y<Z<"+rules;
//        rules="";
//        rules=rb.getRules()+rules;
//        log.info("Rules: "+rules);
//        RuleBasedCollator rb2=new RuleBasedCollator(rules);
        var rb = new RuleBasedCollator(r.getRules() + ",A<a,B<b,C<c,D<d,E<e,F<f,G<g,H<h,I<i,J<j,K<k,L<l,M<m,N<n,O<o,P<p,Q<q,R<r,S<s,T<t,U<u,V<v,W<w,X<x,Y<y,Z<z,Ö<ö,Ü<ü,Ä<ä");
        List<String> lst = Arrays.asList("a", "d", "e", "b", "A", "x", "F", "Z", "D", "ö");
        lst.sort((o1, o2)->rb.compare(o1, o2));

        for (String o : lst) {
            log.info(o);
        }
    }
}
