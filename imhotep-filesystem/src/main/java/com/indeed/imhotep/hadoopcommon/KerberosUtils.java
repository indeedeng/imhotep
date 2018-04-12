/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.hadoopcommon;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.indeed.util.core.NetUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author dwahler
 */
public final class KerberosUtils {
    private static final Logger log = Logger.getLogger(KerberosUtils.class);

    private static final String KERBEROS_PRINCIPAL_KEY = "kerberos.principal";
    private static final String KERBEROS_KEYTAB_KEY = "kerberos.keytab";

    private KerberosUtils() {
    }

    public static void loginFromKeytab(@Nonnull final Configuration props) throws IOException {
        // first try and login from the config
        final String principal = props.getString(KERBEROS_PRINCIPAL_KEY);
        final String keytabPath = props.getString(KERBEROS_KEYTAB_KEY);

        loginFromKeytab(principal, keytabPath);
    }

    public static void loginFromKeytab(@Nonnull final Map<String, ?> configuration) throws IOException {
        final String principal = (String) configuration.get(KERBEROS_PRINCIPAL_KEY);
        final String keytabPath = (String) configuration.get(KERBEROS_KEYTAB_KEY);
        loginFromKeytab(principal, keytabPath);
    }

    public static void loginFromKeytab(@Nullable final String principal, @Nullable final String keytabPath) throws IOException {
        // if one of these is set, then assume they meant to set both
        if (!Strings.isNullOrEmpty(principal) || !Strings.isNullOrEmpty(keytabPath)) {
            log.info("Using properties from configuration file");
            with(principal, keytabPath);
        } else {
            // look for the file to be readable for this user
            final String username = System.getProperty("user.name");
            final File keytabFile = new File(String.format("/etc/local_keytabs/%1$s/%1$s.keytab",
                                                           username));
            log.info("Checking for user " + username + " keytab, settings at " + keytabFile.getAbsolutePath());
            if (keytabFile.exists() && keytabFile.canRead()) {
                with(username + "/_HOST@INDEED.NET", keytabFile.getAbsolutePath());
            } else {
                log.warn("Unable to find user keytab, maybe the keytab is in ticket from klist");
            }
        }
    }

    private static void with(final String principal, final String keytabPath) throws IOException {
        log.info("Setting keytab file of " + keytabPath + ", and principal to " + principal);
        checkArgument(!Strings.isNullOrEmpty(principal),
                      "Unable to use a null/empty principal for keytab");
        checkArgument(!Strings.isNullOrEmpty(keytabPath), "Unable to use a null/empty keytab path");

        // do hostname substitution
        final String realPrincipal = SecurityUtil.getServerPrincipal(principal, (String) null);
        // actually login
        try {
            UserGroupInformation.loginUserFromKeytab(realPrincipal, keytabPath);
        } catch (final IOException e) {
            checkKnownErrors(realPrincipal, e);
            throw e;
        }
    }

    private static void checkKnownErrors(final String realPrincipal, final IOException e) throws
            IOException {
        // if it contains the ip address this is bad
        if (realPrincipal.contains(NetUtils.determineIpAddress())) {
            throw new IOException(
                    "Hostname substitution failed, probably because reverse DNS is not setup", e);
        }

        // we did not see the ip address so compare CanonicalHostName to HostName
        final String hostname = NetUtils.determineHostName();
        final String canonicalHostname = getCanonicalHostNameSafe();
        if (!hostname.equalsIgnoreCase(canonicalHostname)) {
            throw new IOException(
                    "Unable to match canonicalHostname, maybe reverse DNS is wrong? saw " + canonicalHostname,
                    e);
        }
    }

    @Nullable
    private static String getCanonicalHostNameSafe() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (final UnknownHostException e) {
            log.error("Unable to lookup hostname of machine " + e.getMessage());
            return null;
        }
    }

    /**
     * Use for testing keytab logins
     */
    public static void main(final String[] args) throws IOException {
        KerberosUtils.loginFromKeytab(new BaseConfiguration());
        final FileSystem fileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
        final Path path = new Path("/CLUSTERNAME");
        if (fileSystem.exists(path)) {
            //noinspection UseOfSystemOutOrSystemErr
            System.out.println(CharStreams.toString(new InputStreamReader(fileSystem.open(path),
                                                                          Charsets.UTF_8)));
        }
    }
}
