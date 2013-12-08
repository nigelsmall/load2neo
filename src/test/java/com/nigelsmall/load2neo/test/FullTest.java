/*
 * Copyright 2013, Nigel Small
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nigelsmall.load2neo.test;

import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.Enumeration;

public class FullTest {

    @Test
    public void run() throws IOException {
        Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("test.geoff");
        while (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            File file = new File(url.getPath());
            System.out.println(file.getName());
            FileReader reader = new FileReader(file);
            DocumentTest test = new DocumentTest(reader);
            test.run();
        }
    }

}
