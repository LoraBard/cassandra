/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.FileUtils;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

public class FBUtilities
{
    private static final Logger logger = LoggerFactory.getLogger(FBUtilities.class);

    private static final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    public static final BigInteger TWO = new BigInteger("2");
    private static final String DEFAULT_TRIGGER_DIR = "triggers";

    public static final String OPERATING_SYSTEM = System.getProperty("os.name").toLowerCase();
    public static final boolean isWindows = OPERATING_SYSTEM.contains("windows");
    public static final boolean isLinux = OPERATING_SYSTEM.contains("linux");
    public static final boolean isMacOSX = OPERATING_SYSTEM.contains("mac os x");

    private static volatile InetAddress localInetAddress;
    private static volatile InetAddress broadcastInetAddress;
    private static volatile InetAddress broadcastRpcAddress;

    public static int getAvailableProcessors()
    {
        String availableProcessors = System.getProperty("cassandra.available_processors");
        if (!Strings.isNullOrEmpty(availableProcessors))
            return Integer.parseInt(availableProcessors);
        else
            return Runtime.getRuntime().availableProcessors();
    }

    public static final int MAX_UNSIGNED_SHORT = 0xFFFF;

    /**
     * Please use getBroadcastAddress instead. You need this only when you have to listen/connect.
     */
    public static InetAddress getLocalAddress()
    {
        if (localInetAddress == null)
            try
            {
                localInetAddress = DatabaseDescriptor.getListenAddress() == null
                                    ? InetAddress.getLocalHost()
                                    : DatabaseDescriptor.getListenAddress();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        return localInetAddress;
    }

    public static InetAddress getBroadcastAddress()
    {
        if (broadcastInetAddress == null)
            broadcastInetAddress = DatabaseDescriptor.getBroadcastAddress() == null
                                 ? getLocalAddress()
                                 : DatabaseDescriptor.getBroadcastAddress();
        return broadcastInetAddress;
    }


    public static InetAddress getNativeTransportBroadcastAddress()
    {
        if (broadcastRpcAddress == null)
            broadcastRpcAddress = DatabaseDescriptor.getBroadcastNativeTransportAddress() == null
                                   ? DatabaseDescriptor.getNativeTransportAddress()
                                   : DatabaseDescriptor.getBroadcastNativeTransportAddress();
        return broadcastRpcAddress;
    }

    public static Collection<InetAddress> getAllLocalAddresses()
    {
        Set<InetAddress> localAddresses = new HashSet<InetAddress>();
        try
        {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            if (nets != null)
            {
                while (nets.hasMoreElements())
                    localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
            }
        }
        catch (SocketException e)
        {
            throw new AssertionError(e);
        }
        return localAddresses;
    }

    public static String getNetworkInterface(InetAddress localAddress)
    {
        try
        {
            for(NetworkInterface ifc : Collections.list(NetworkInterface.getNetworkInterfaces()))
            {
                if(ifc.isUp())
                {
                    for(InetAddress addr : Collections.list(ifc.getInetAddresses()))
                    {
                        if (addr.equals(localAddress))
                            return ifc.getDisplayName();
                    }
                }
            }
        }
        catch (SocketException e) {}
        return null;
    }

    /**
     * Given two bit arrays represented as BigIntegers, containing the given
     * number of significant bits, calculate a midpoint.
     *
     * @param left The left point.
     * @param right The right point.
     * @param sigbits The number of bits in the points that are significant.
     * @return A midpoint that will compare bitwise halfway between the params, and
     * a boolean representing whether a non-zero lsbit remainder was generated.
     */
    public static Pair<BigInteger,Boolean> midpoint(BigInteger left, BigInteger right, int sigbits)
    {
        BigInteger midpoint;
        boolean remainder;
        if (left.compareTo(right) < 0)
        {
            BigInteger sum = left.add(right);
            remainder = sum.testBit(0);
            midpoint = sum.shiftRight(1);
        }
        else
        {
            BigInteger max = TWO.pow(sigbits);
            // wrapping case
            BigInteger distance = max.add(right).subtract(left);
            remainder = distance.testBit(0);
            midpoint = distance.shiftRight(1).add(left).mod(max);
        }
        return Pair.create(midpoint, remainder);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2, int offset1, int offset2, int len1, int len2)
    {
        return FastByteOperations.compareUnsigned(bytes1, offset1, len1, bytes2, offset2, len2);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2)
    {
        return compareUnsigned(bytes1, bytes2, 0, 0, bytes1.length, bytes2.length);
    }

    /**
     * @return The bitwise XOR of the inputs. The output will be the same length as the
     * longer input, but if either input is null, the output will be null.
     */
    public static byte[] xor(byte[] left, byte[] right)
    {
        if (left == null || right == null)
            return null;
        if (left.length > right.length)
        {
            byte[] swap = left;
            left = right;
            right = swap;
        }

        // left.length is now <= right.length
        byte[] out = Arrays.copyOf(right, right.length);
        for (int i = 0; i < left.length; i++)
        {
            out[i] = (byte)((left[i] & 0xFF) ^ (right[i] & 0xFF));
        }
        return out;
    }

    public static void sortSampledKeys(List<DecoratedKey> keys, Range<Token> range)
    {
        if (range.left.compareTo(range.right) >= 0)
        {
            // range wraps.  have to be careful that we sort in the same order as the range to find the right midpoint.
            final Token right = range.right;
            Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
            {
                public int compare(DecoratedKey o1, DecoratedKey o2)
                {
                    if ((right.compareTo(o1.getToken()) < 0 && right.compareTo(o2.getToken()) < 0)
                        || (right.compareTo(o1.getToken()) > 0 && right.compareTo(o2.getToken()) > 0))
                    {
                        // both tokens are on the same side of the wrap point
                        return o1.compareTo(o2);
                    }
                    return o2.compareTo(o1);
                }
            };
            Collections.sort(keys, comparator);
        }
        else
        {
            // unwrapped range (left < right).  standard sort is all we need.
            Collections.sort(keys);
        }
    }

    public static String resourceToFile(String filename) throws ConfigurationException
    {
        ClassLoader loader = FBUtilities.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + filename);

        return new File(scpurl.getFile()).getAbsolutePath();
    }

    public static File cassandraTriggerDir()
    {
        File triggerDir = null;
        if (System.getProperty("cassandra.triggers_dir") != null)
        {
            triggerDir = new File(System.getProperty("cassandra.triggers_dir"));
        }
        else
        {
            URL confDir = FBUtilities.class.getClassLoader().getResource(DEFAULT_TRIGGER_DIR);
            if (confDir != null)
                triggerDir = new File(confDir.getFile());
        }
        if (triggerDir == null || !triggerDir.exists())
        {
            logger.warn("Trigger directory doesn't exist, please create it and try again.");
            return null;
        }
        return triggerDir;
    }

    public static String getReleaseVersionString()
    {
        try (InputStream in = FBUtilities.class.getClassLoader().getResourceAsStream("org/apache/cassandra/config/version.properties"))
        {
            if (in == null)
            {
                return System.getProperty("cassandra.releaseVersion", "Unknown");
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("CassandraVersion");
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Unable to load version.properties", e);
            return "debug version";
        }
    }

    public static long timestampMicros()
    {
        // we use microsecond resolution for compatibility with other client libraries, even though
        // we can't actually get microsecond precision.
        return System.currentTimeMillis() * 1000;
    }

    public static int nowInSeconds()
    {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static <T> List<T> waitOnFutures(Iterable<? extends Future<? extends T>> futures)
    {
        return waitOnFutures(futures, -1);
    }

    /**
     * Block for a collection of futures, with an optional timeout for each future.
     *
     * @param futures
     * @param ms The number of milliseconds to wait on each future. If this value is less than or equal to zero,
     *           no tiemout value will be passed to {@link Future#get()}.
     */
    public static <T> List<T> waitOnFutures(Iterable<? extends Future<? extends T>> futures, long ms)
    {
        List<T> results = new ArrayList<>();
        Throwable fail = null;
        for (Future<? extends T> f : futures)
        {
            try
            {
                if (ms <= 0)
                    results.add(f.get());
                else
                    results.add(f.get(ms, TimeUnit.MILLISECONDS));
            }
            catch (Throwable t)
            {
                fail = Throwables.merge(fail, t);
            }
        }
        Throwables.maybeFail(fail);
        return results;
    }

    public static <T> T waitOnFuture(Future<T> future)
    {
        try
        {
            return future.get();
        }
        catch (ExecutionException ee)
        {
            throw new RuntimeException(ee);
        }
        catch (InterruptedException ie)
        {
            throw new AssertionError(ie);
        }
    }

    public static <T> Future<? extends T> waitOnFirstFuture(Iterable<? extends Future<? extends T>> futures)
    {
        return waitOnFirstFuture(futures, 100);
    }

    /**
     * Only wait for the first future to finish from a list of futures. Will block until at least 1 future finishes.
     * @param futures The futures to wait on
     * @return future that completed.
     */
    public static <T> Future<? extends T> waitOnFirstFuture(Iterable<? extends Future<? extends T>> futures, long delay)
    {
        while (true)
        {
            for (Future<? extends T> f : futures)
            {
                if (f.isDone())
                {
                    try
                    {
                        f.get();
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                    catch (ExecutionException e)
                    {
                        throw new RuntimeException(e);
                    }
                    return f;
                }
            }
            Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Create a new instance of a partitioner defined in an SSTable Descriptor
     * @param desc Descriptor of an sstable
     * @return a new IPartitioner instance
     * @throws IOException
     */
    public static IPartitioner newPartitioner(Descriptor desc) throws IOException
    {
        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        return newPartitioner(validationMetadata.partitioner, Optional.of(header.getKeyType()));
    }

    public static IPartitioner newPartitioner(String partitionerClassName) throws ConfigurationException
    {
        return newPartitioner(partitionerClassName, Optional.empty());
    }

    @VisibleForTesting
    static IPartitioner newPartitioner(String partitionerClassName, Optional<AbstractType<?>> comparator) throws ConfigurationException
    {
        if (!partitionerClassName.contains("."))
            partitionerClassName = "org.apache.cassandra.dht." + partitionerClassName;

        if (partitionerClassName.equals("org.apache.cassandra.dht.LocalPartitioner"))
        {
            assert comparator.isPresent() : "Expected a comparator for local partitioner";
            return new LocalPartitioner(comparator.get());
        }
        return FBUtilities.instanceOrConstruct(partitionerClassName, "partitioner");
    }

    public static IAuthorizer newAuthorizer(String className) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        return FBUtilities.construct(className, "authorizer");
    }

    public static IAuthenticator newAuthenticator(String className) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        return FBUtilities.construct(className, "authenticator");
    }

    public static IRoleManager newRoleManager(String className) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        return FBUtilities.construct(className, "role manager");
    }

    /**
     * @return The Class for the given name.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> Class<T> classForName(String classname, String readable) throws ConfigurationException
    {
        try
        {
            return (Class<T>)Class.forName(classname);
        }
        catch (ClassNotFoundException | NoClassDefFoundError e)
        {
            throw new ConfigurationException(String.format("Unable to find %s class '%s'", readable, classname), e);
        }
    }

    /**
     * Constructs an instance of the given class, which must have a no-arg or default constructor.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> T instanceOrConstruct(String classname, String readable) throws ConfigurationException
    {
        Class<T> cls = FBUtilities.classForName(classname, readable);
        try
        {
            Field instance = cls.getField("instance");
            return cls.cast(instance.get(null));
        }
        catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e)
        {
            // Could not get instance field. Try instantiating.
            return construct(cls, classname, readable);
        }
    }

    /**
     * Constructs an instance of the given class, which must have a no-arg or default constructor.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> T construct(String classname, String readable) throws ConfigurationException
    {
        Class<T> cls = FBUtilities.classForName(classname, readable);
        return construct(cls, classname, readable);
    }

    private static <T> T construct(Class<T> cls, String classname, String readable) throws ConfigurationException
    {
        try
        {
            return cls.newInstance();
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException(String.format("Default constructor for %s class '%s' is inaccessible.", readable, classname));
        }
        catch (InstantiationException e)
        {
            throw new ConfigurationException(String.format("Cannot use abstract class '%s' as %s.", classname, readable));
        }
        catch (Exception e)
        {
            // Catch-all because Class.newInstance() "propagates any exception thrown by the nullary constructor, including a checked exception".
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException)e.getCause();
            throw new ConfigurationException(String.format("Error instantiating %s class '%s'.", readable, classname), e);
        }
    }

    public static <T> NavigableSet<T> singleton(T column, Comparator<? super T> comparator)
    {
        NavigableSet<T> s = new TreeSet<T>(comparator);
        s.add(column);
        return s;
    }

    public static <T> NavigableSet<T> emptySortedSet(Comparator<? super T> comparator)
    {
        return new TreeSet<T>(comparator);
    }

    /**
     * Make straing out of the given {@code Map}.
     *
     * @param map Map to make string.
     * @return String representation of all entries in the map,
     *         where key and value pair is concatenated with ':'.
     */
    @Nonnull
    public static String toString(@Nullable Map<?, ?> map)
    {
        if (map == null)
            return "";
        Joiner.MapJoiner joiner = Joiner.on(", ").withKeyValueSeparator(":");
        return joiner.join(map);
    }

    /**
     * Used to get access to protected/private field of the specified class
     * @param className the class name
     * @param fieldName the field name
     * @return the requested {@code Field}
     */
    public static Field getProtectedField(String className, String fieldName)
    {
        try
        {
            return getProtectedField(Class.forName(className), fieldName);
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Used to get access to protected/private field of the specified class
     * @param klass the class
     * @param fieldName the field name
     * @return the requested Field
     */
    public static Field getProtectedField(Class<?> klass, String fieldName)
    {
        try
        {
            Field field = klass.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator)
    {
        return new WrappedCloseableIterator<T>(iterator);
    }

    public static Map<String, String> fromJsonMap(String json)
    {
        try
        {
            return jsonMapper.readValue(json, Map.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static List<String> fromJsonList(String json)
    {
        try
        {
            return jsonMapper.readValue(json, List.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String json(Object object)
    {
        try
        {
            return jsonMapper.writeValueAsString(object);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String prettyPrintMemory(long size)
    {
        return prettyPrintMemory(size, false);
    }

    public static String prettyPrintMemory(long size, boolean includeSpace)
    {
        if (size >= 1 << 30)
            return String.format("%.3f%sGiB", size / (double) (1 << 30), includeSpace ? " " : "");
        if (size >= 1 << 20)
            return String.format("%.3f%sMiB", size / (double) (1 << 20), includeSpace ? " " : "");
        return String.format("%.3f%sKiB", size / (double) (1 << 10), includeSpace ? " " : "");
    }

    public static String prettyPrintMemoryPerSecond(long rate)
    {
        if (rate >= 1 << 30)
            return String.format("%.3fGiB/s", rate / (double) (1 << 30));
        if (rate >= 1 << 20)
            return String.format("%.3fMiB/s", rate / (double) (1 << 20));
        return String.format("%.3fKiB/s", rate / (double) (1 << 10));
    }

    public static String prettyPrintMemoryPerSecond(long bytes, long timeInNano)
    {
        // We can't sanely calculate a rate over 0 nanoseconds
        if (timeInNano == 0)
            return "NaN  KiB/s";

        long rate = (long) (((double) bytes / timeInNano) * 1000 * 1000 * 1000);

        return prettyPrintMemoryPerSecond(rate);
    }

    /**
     * Starts and waits for the given @param pb to finish.
     * @throws java.io.IOException on non-zero exit code
     */
    public static void exec(ProcessBuilder pb) throws IOException
    {
        Process p = pb.start();
        try
        {
            int errCode = p.waitFor();
            if (errCode != 0)
            {
            	try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                     BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream())))
                {
            		String lineSep = System.getProperty("line.separator");
	                StringBuilder sb = new StringBuilder();
	                String str;
	                while ((str = in.readLine()) != null)
	                    sb.append(str).append(lineSep);
	                while ((str = err.readLine()) != null)
	                    sb.append(str).append(lineSep);
	                throw new IOException("Exception while executing the command: "+ StringUtils.join(pb.command(), " ") +
	                                      ", command error Code: " + errCode +
	                                      ", command output: "+ sb.toString());
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    public static void updateChecksumInt(Checksum checksum, int v)
    {
        checksum.update((v >>> 24) & 0xFF);
        checksum.update((v >>> 16) & 0xFF);
        checksum.update((v >>> 8) & 0xFF);
        checksum.update((v >>> 0) & 0xFF);
    }

    /**
      * Updates checksum with the provided ByteBuffer at the given offset + length.
      * Resets position and limit back to their original values on return.
      * This method is *NOT* thread-safe.
      */
    public static void updateChecksum(CRC32 checksum, ByteBuffer buffer, int offset, int length)
    {
        int position = buffer.position();
        int limit = buffer.limit();

        buffer.position(offset).limit(offset + length);
        checksum.update(buffer);

        buffer.position(position).limit(limit);
    }

    /**
     * Updates checksum with the provided ByteBuffer.
     * Resets position back to its original values on return.
     * This method is *NOT* thread-safe.
     */
    public static void updateChecksum(CRC32 checksum, ByteBuffer buffer)
    {
        int position = buffer.position();
        checksum.update(buffer);
        buffer.position(position);
    }

    public static long abs(long index)
    {
        long negbit = index >> 63;
        return (index ^ negbit) - negbit;
    }

    private static final class WrappedCloseableIterator<T>
        extends AbstractIterator<T> implements CloseableIterator<T>
    {
        private final Iterator<T> source;
        public WrappedCloseableIterator(Iterator<T> source)
        {
            this.source = source;
        }

        protected T computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            return source.next();
        }

        public void close() {}
    }

    public static long copy(InputStream from, OutputStream to, long limit) throws IOException
    {
        byte[] buffer = new byte[64]; // 64 byte buffer
        long copied = 0;
        int toCopy = buffer.length;
        while (true)
        {
            if (limit < buffer.length + copied)
                toCopy = (int) (limit - copied);
            int sofar = from.read(buffer, 0, toCopy);
            if (sofar == -1)
                break;
            to.write(buffer, 0, sofar);
            copied += sofar;
            if (limit == copied)
                break;
        }
        return copied;
    }

    public static File getToolsOutputDirectory()
    {
        File historyDir = new File(System.getProperty("user.home"), ".cassandra");
        FileUtils.createDirectory(historyDir);
        return historyDir;
    }

    public static void closeAll(Collection<? extends AutoCloseable> l) throws Exception
    {
        Exception toThrow = null;
        for (AutoCloseable c : l)
        {
            try
            {
                c.close();
            }
            catch (Exception e)
            {
                if (toThrow == null)
                    toThrow = e;
                else
                    toThrow.addSuppressed(e);
            }
        }
        if (toThrow != null)
            throw toThrow;
    }

    public static byte[] toWriteUTFBytes(String s)
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeUTF(s);
            dos.flush();
            return baos.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

	public static void sleepQuietly(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static long align(long val, int boundary)
    {
        return (val + boundary - 1) & ~(boundary - 1);
    }

    @VisibleForTesting
    protected static void reset()
    {
        localInetAddress = null;
        broadcastInetAddress = null;
        broadcastRpcAddress = null;
    }

    /**
     * Return the sum of its arguments or Long.MAX_VALUE on overflow.
     */
    public static long add(long x, long y)
    {
        long r = x + y;
        // HD 2-12 Overflow iff both arguments have the opposite sign of the result, see Math.addExact()
        if (((x ^ r) & (y ^ r)) < 0)
        {
            return Long.MAX_VALUE;
        }

        return r;
    }

    /**
     * Return the sum of its arguments or Integer.MAX_VALUE on overflow.
     */
    public static int add(int x, int y)
    {
        int r = x + y;
        // HD 2-12 Overflow iff both arguments have the opposite sign of the result, see Math.addExact()
        if (((x ^ r) & (y ^ r)) < 0)
        {
            return Integer.MAX_VALUE;
        }

        return r;
    }

    /**
     * A class containing some debug methods to be added and removed manually when debugging problems
     * like failing unit tests.
     */
    public static final class Debug
    {
        public static final class ThreadInfo
        {
            private final String name;
            private final boolean isDaemon;
            private final StackTraceElement[] stack;

            public ThreadInfo()
            {
                this(Thread.currentThread());
            }

            public ThreadInfo(Thread thread)
            {
                this.name =  thread.getName();
                this.isDaemon = thread.isDaemon();
                this.stack = thread.getStackTrace();
            }

        }
        private static final Map<Object, ThreadInfo> stacks = new ConcurrentHashMap<>();

        public static String getStackTrace()
        {
            return getStackTrace(new ThreadInfo());
        }

        public static String getStackTrace(Thread thread)
        {
            return getStackTrace(new ThreadInfo(thread));
        }

        public static String getStackTrace(ThreadInfo threadInfo)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("Thread ")
              .append(threadInfo.name)
              .append(" (")
              .append(threadInfo.isDaemon ? "daemon" : "non-daemon")
              .append(")")
              .append("\n");
            for (StackTraceElement element : threadInfo.stack)
            {
                sb.append(element);
                sb.append("\n");
            }
            return sb.toString();
        }

        /**
         * Call this method for debugging purposes, when you want to save the current thread stack trace
         * for the object specified in the parameter.
         */
        public static void addStackTrace(Object object)
        {
            stacks.put(object, new ThreadInfo());
        }


        /** Call this method to log a message that will print the stack trace that was saved
         * in the last call of addStackTrace() and the current stack trace.
         */
        public static void logStackTrace(String message, Object object)
        {
            logger.info("{}\n{}\n****\n{}",
                        message,
                        getStackTrace(stacks.get(object)),
                        getStackTrace());
        }
    }

    public static class CpuInfo
    {
        private final List<PhysicalProcessor> processors = new ArrayList<>();

        @VisibleForTesting
        protected CpuInfo()
        {
        }

        public static String niceCpuIdList(List<Integer> cpuIds)
        {
            StringBuilder sb = new StringBuilder();
            Integer rangeStart = null;
            Integer previous = null;
            Integer id;
            for (Iterator<Integer> i = cpuIds.iterator(); ; previous = id)
            {
                if (!i.hasNext())
                {
                    if (rangeStart == null)
                        break;
                    if (sb.length() > 0)
                        sb.append(',');
                    if (previous.equals(rangeStart))
                        sb.append(rangeStart);
                    else
                        sb.append(rangeStart).append('-').append(previous);
                    break;
                }
                id = i.next();
                if (rangeStart == null)
                {
                    rangeStart = id;
                }
                else
                {
                    if (previous + 1 == id)
                    {
                        // range continues
                        continue;
                    }
                    // range ended
                    if (sb.length() > 0)
                        sb.append(',');
                    if (previous.equals(rangeStart))
                        sb.append(rangeStart);
                    else
                        sb.append(rangeStart).append('-').append(previous);
                    rangeStart = id;
                }
            }
            return sb.toString();
        }

        public List<PhysicalProcessor> getProcessors()
        {
            return processors;
        }

        public static CpuInfo load()
        {
            File fCpuInfo = new File("/proc/cpuinfo");
            if (!fCpuInfo.exists())
                throw new IOError(new FileNotFoundException(fCpuInfo.getAbsolutePath()));
            List<String> cpuinfoLines = FileUtils.readLines(fCpuInfo);

            return loadFrom(cpuinfoLines);
        }

        public static CpuInfo loadFrom(List<String> lines)
        {
            CpuInfo cpuInfo = new CpuInfo();

            Pattern linePattern = Pattern.compile("^([A-Za-z _-]+[A-Za-z_-])\\s*[:] (.*)$");
            Map<String, String> info = new HashMap<>();
            for (String cpuinfoLine : lines)
            {
                if (cpuinfoLine.isEmpty())
                {
                    cpuInfo.addCpu(info);
                    info.clear();
                }
                else
                {
                    Matcher matcher = linePattern.matcher(cpuinfoLine);
                    if (matcher.matches())
                        info.put(matcher.group(1), matcher.group(2));
                }
            }
            cpuInfo.addCpu(info);

            return cpuInfo;
        }

        private void addCpu(Map<String, String> info)
        {
            if (info.isEmpty())
                return;

            try
            {
                int physicalId = Integer.parseInt(info.get("physical id"));
                int cores = Integer.parseInt(info.get("cpu cores"));
                int cpuId = Integer.parseInt(info.get("processor"));
                int coreId = Integer.parseInt(info.get("core id"));
                String modelName = info.get("model name");
                String mhz = info.get("cpu MHz");
                String cacheSize = info.get("cache size");
                String vendorId = info.get("vendor_id");
                Set<String> flags = new HashSet<>(Arrays.asList(info.get("flags").split(" ")));

                PhysicalProcessor processor = getProcessor(physicalId);
                if (processor == null)
                {
                    processor = new PhysicalProcessor(modelName, physicalId, mhz, cacheSize, vendorId, flags, cores);
                    processors.add(processor);
                }
                processor.addCpu(coreId, cpuId);
            }
            catch (Exception e)
            {
                // ignore - parsing errors for /proc/cpuinfo are not that relevant...
            }
        }

        private PhysicalProcessor getProcessor(int physicalId)
        {
            return processors.stream()
                             .filter(p -> p.physicalId == physicalId)
                             .findFirst()
                             .orElse(null);
        }

        public String fetchCpuScalingGovernor(int cpuId)
        {
            File cpuDir = new File(String.format("/sys/devices/system/cpu/cpu%d/cpufreq", cpuId));
            if (!cpuDir.canRead())
                return "no_cpufreq";
            File file = new File(cpuDir, "scaling_governor");
            if (!file.canRead())
                return "no_scaling_governor";
            String governor = FileUtils.readLine(file);
            return governor != null ? governor : "unknown";
        }

        public int cpuCount()
        {
            return processors.stream()
                      .mapToInt(PhysicalProcessor::cpuCount)
                      .sum();
        }

        public static class PhysicalProcessor
        {
            private final int physicalId;
            private final String name;
            private final String mhz;
            private final String cacheSize;
            private final String vendorId;
            private final Set<String> flags;
            private final int cores;
            private final BitSet cpuIds = new BitSet();
            private final BitSet coreIds = new BitSet();

            PhysicalProcessor(String name, int physicalId, String mhz, String cacheSize, String vendorId, Set<String> flags, int cores)
            {
                this.name = name;
                this.physicalId = physicalId;
                this.mhz = mhz;
                this.cacheSize = cacheSize;
                this.vendorId = vendorId;
                this.flags = flags;
                this.cores = cores;
            }

            public String getName()
            {
                return name;
            }

            public int getPhysicalId()
            {
                return physicalId;
            }

            public String getMhz()
            {
                return mhz;
            }

            public int getCores()
            {
                return cores;
            }

            public String getCacheSize()
            {
                return cacheSize;
            }

            public String getVendorId()
            {
                return vendorId;
            }

            public Set<String> getFlags()
            {
                return flags;
            }

            public boolean hasFlag(String flag)
            {
                return flags.contains(flag);
            }

            public int getThreadsPerCore()
            {
                return cpuIds.cardinality() / coreIds.cardinality();
            }

            public IntStream cpuIds()
            {
                return cpuIds.stream();
            }

            void addCpu(int coreId, int cpuId)
            {
                cpuIds.set(cpuId);
                coreIds.set(coreId);
            }

            public int cpuCount()
            {
                return cpuIds.cardinality();
            }
        }
    }

    /**
     * Executes the specified command in a separate process. Waits for the process to complete
     * and returns the entire standard output as a string.
     *
     * This method should only be used for executing shell commands
     * that return a small output. This method will also block the calling thread.
     *
     * @param cmd - the command to execute
     * @param timeout - the maximum time to wait for the command to complete
     * @param timeUnit - the time unit for the timeout
     *
     * @return - the command standard output
     * @throws RuntimeException if the program cannot be executed or does not complete within the specified timeout
     */
    public static String execBlocking(String[] cmd, int timeout, TimeUnit timeUnit)
    {
        try
        {
            Process proc = Runtime.getRuntime().exec(cmd);
            if (!proc.waitFor(timeout, timeUnit))
            {
                String out = toString(proc.getInputStream());
                proc.destroy();
                throw new RuntimeException(String.format("<%s> did not terminate within %d %s. Partial output:\n%s",
                                                         cmd, timeout, timeUnit.name().toLowerCase(), out));
            }

            if (proc.exitValue() != 0)
            {
                throw new RuntimeException(String.format("<%s> failed with error code %d:\n%s",
                                                         cmd, proc.exitValue(), toString(proc.getErrorStream())));
            }

            return toString(proc.getInputStream());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String toString(InputStream in) throws IOException
    {
        byte[] buf = new byte[in.available()];
        for (int p = 0; p < buf.length; )
        {
            int rd = in.read(buf, p, buf.length - p);
            if (rd < 0)
                break;
            p += rd;
        }
        return new String(buf, StandardCharsets.UTF_8);
    }
}
