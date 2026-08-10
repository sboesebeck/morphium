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

import de.caluga.morphium.annotations.*;

/**
 * Test entity that contains an {@link AddressEmbedded} sub-document.
 */
@Entity(collectionName = "it_customers")
public class CustomerEntity {

    @Id
    private String id;

    @Property(fieldName = "name")
    private String name;

    @Property(fieldName = "address")
    private AddressEmbedded address;

    public String          getId()                  { return id; }
    public void            setId(String id)         { this.id = id; }
    public String          getName()                { return name; }
    public void            setName(String name)     { this.name = name; }
    public AddressEmbedded getAddress()             { return address; }
    public void            setAddress(AddressEmbedded a) { this.address = a; }
}
