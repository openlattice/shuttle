package com.openlattice.shuttle.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jetbrains.annotations.NotNull;

import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cached {

    private static final LoadingCache<String, Matcher> regexCache = buildRegexCache();
    private static final LoadingCache<String, Matcher> insensitiveRegexCache = buildInsensitiveRegexCache();
    private static final LoadingCache<String, DateTimeFormatter> formatCache = buildFormatCache();

    private static LoadingCache<String, Matcher> buildRegexCache() {
        return CacheBuilder.newBuilder()
                .build( CacheLoader.from( (key) -> Pattern.compile( key ).matcher( "" ) ) );
    }

    private static LoadingCache<String, Matcher> buildInsensitiveRegexCache() {
        return CacheBuilder.newBuilder()
                .build( CacheLoader.from( (key) -> Pattern.compile( key, Pattern.CASE_INSENSITIVE ).matcher( "" ) ) );
    }

    private static LoadingCache<String, DateTimeFormatter> buildFormatCache() {
        return CacheBuilder.newBuilder()
                .build( CacheLoader.from( DateTimeFormatter::ofPattern ) );
    }

    public static Matcher getInsensitiveMatcherForString( @NotNull String targetString, @NotNull String regex ) {
        Matcher unchecked = insensitiveRegexCache.getUnchecked( regex );
        unchecked.reset( targetString );
        return unchecked;
    }

    public static Matcher getMatcherForString( @NotNull String targetString, @NotNull String regex ) {
        Matcher unchecked = regexCache.getUnchecked( regex );
        unchecked.reset( targetString );
        return unchecked;
    }

    public static DateTimeFormatter getDateFormatForString( @NotNull String dateFormatString ) throws ExecutionException {
        return formatCache.get( dateFormatString );
    }
}
