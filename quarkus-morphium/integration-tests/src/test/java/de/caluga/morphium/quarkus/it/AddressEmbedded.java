/*
 * Copyright 2025 The Quarkiverse Authors
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
package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;

/**
 * Embedded address document used in embedded-document integration tests.
 */
@Embedded
public class AddressEmbedded {

    @Property(fieldName = "street")
    private String street;

    @Property(fieldName = "city")
    private String city;

    @Property(fieldName = "zip")
    private String zip;

    public String getStreet()              { return street; }
    public void   setStreet(String street) { this.street = street; }
    public String getCity()                { return city; }
    public void   setCity(String city)     { this.city = city; }
    public String getZip()                 { return zip; }
    public void   setZip(String zip)       { this.zip = zip; }
}
