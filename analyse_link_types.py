from hdt import HDTDocument
import csv
import gzip
import glob
from urllib.parse import urlparse
import urllib.parse

HDTDIRECTORY = 'HDTfiles'

# Read all HDT files in HDTfiles directory
for filepath in glob.iglob(HDTDIRECTORY + '/*.hdt'):

    # Loading authoritative URIs from generated CSV file
    with open('authoritative_all.csv') as csvfile:
        data = list(csv.reader(csvfile, delimiter=';'))
    authoritative_namespace_uris = dict()

    # Get authoritative namespace for each dataset
    for row in data:
        if row[1] in authoritative_namespace_uris:
            namespace_value = authoritative_namespace_uris.get(row[1])
            if row[2] > namespace_value[1]:
                authoritative_namespace_uris[row[1]] = (row[0], row[2])
        else:
            authoritative_namespace_uris[row[1]] = (row[0], row[2])

    for key in authoritative_namespace_uris:

        # Get Pay Level Domain of authoritative namespace
        namespace_authority = authoritative_namespace_uris[key]
        parsed_uri = urlparse(urllib.parse.unquote(namespace_authority[0]))
        spltAr = namespace_authority[0].split("://")
        i = (0, 1)[len(spltAr) > 1]
        authoritative_dataset_URI = "http://" + spltAr[i].split("?")[0].split('/')[0].split(':')[0].lower()
        filename = key
        print(authoritative_dataset_URI)
        print(filename)

        try:
            if filename.endswith("gz"):
                document = HDTDocument(gzip.open(HDTDIRECTORY + '/' + filename, 'rb').read())
            else:
                document = HDTDocument(HDTDIRECTORY + '/' + filename)

            # Initialise sets for classes and properties
            unregistered_classes, unregistered_properties = set(), set()
            unique_classes, unique_properties = set(), set()
            declared_individuals = set()
            reused_individuals = set()
            linked_individuals = set()
            all_classes, all_properties = set(), set()

            # Store some metadata about the HDT document itself
            statistics = { "Number of triples": document.total_triples,
                           "Number of subjects": + document.nb_subjects,
                           "Number of predicates": + document.nb_predicates,
                           "Number of objects": + document.nb_objects,
                           "Number of shared subject-object": + document.nb_shared
                           }

            # Counts
            undeclared_classes_count, declared_classes_count = 0, 0
            declared_properties_count, undeclared_properties_count = 0, 0
            declared_individuals_count, reused_individuals_count, linked_individuals_count = 0, 0, 0
            sameas_link_count, seeAlso_link_count, differentFrom_link_count, allDifferent_link_count = 0, 0, 0, 0
            class_link_count, property_link_count = 0, 0
            instanceTyping_link_count = 0

            # Fetch all declared classes
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Class")
            for triple in triples:
                if triple[0] not in unique_classes:
                    unique_classes.add(triple[0])
                    declared_classes_count += 1
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2002/07/owl#Class")
            for triple in triples:
                if triple[0] not in unique_classes:
                    unique_classes.add(triple[0])
                    declared_classes_count += 1

            # Fetch all used classes
            # Fetch all instances of a class
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "")
            for triple in triples:
                # Fetch all classes in object position
                if triple[2] not in unique_classes:
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1

                # Fetch all unique individuals in subject position
                if triple[0] not in declared_individuals:
                    declared_individuals.add(triple[0])
                    declared_individuals_count += 1
                    if authoritative_dataset_URI not in triple[0]:
                        linked_individuals_count += 1
                        linked_individuals.add(triple[0])

                # Count all instance typing links
                if authoritative_dataset_URI not in triple[2]:
                    instanceTyping_link_count += 1

            # Fetch all SubClassOf relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2000/01/rdf-schema#SubClassOf", "")
            for triple in triples:
                # add classes in subject position
                if triple[0] not in unique_classes:
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1
                # add classes in objects position
                if triple[2] not in unique_classes:
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1

            # Fetch properties and their domain classes
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2000/01/rdf-schema#domain", "")
            for triple in triples:
                if triple[2] not in unique_classes:
                    # add class in object position
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1
                # Fetch all domain properties
                if triple[0] not in unique_properties:
                    # add property in subject position
                    unique_properties.add(triple[0])
                    undeclared_properties_count += 1

            # Fetch properties and their range classes
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2000/01/rdf-schema#range", "")
            for triple in triples:
                if triple[2] not in unique_classes:
                    # add class in object position
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1
                # Fetch all range properties
                if triple[0] not in unique_properties:
                    # add property in subject position
                    unique_properties.add(triple[0])
                    undeclared_properties_count += 1

            # Fetch all local existential guarded class restrictions
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#someValuesFrom", "")
            for triple in triples:
                if triple[2] not in unique_classes:
                    # add class in object position
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1
                # Fetch all owlSomeValuesFrom properties
                if triple[0] not in unique_properties:
                    # add property in subject position
                    unique_properties.add(triple[0])
                    undeclared_properties_count += 1

            # Fetch all local universal guarded class restrictions
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#allValuesFrom", "")
            for triple in triples:
                if triple[2] not in unique_classes:
                    # add object
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1
                # Fetch all owlallValuesFrom properties
                if triple[0] not in unique_properties:
                    # add property in object position
                    unique_properties.add(triple[0])
                    undeclared_properties_count += 1

            # Fetch all equivalentClass relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#equivalentClass ", "")
            for triple in triples:
                if triple[2] not in unique_classes:
                    # add class in object position
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1
                if triple[0] not in unique_classes:
                    # add class in subject position
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1

            # Fetch all disjointWith relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#disjointWith ", "")
            for triple in triples:
                if triple[2] not in unique_classes:
                    # add class in object position
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1
                if triple[0] not in unique_classes:
                    # add class in subject position
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1

            # Fetch all disjointUnionWith relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#disjointUnionOf ", "")
            for triple in triples:
                if triple[0] not in unique_classes:
                    # add class in subject position
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1

            # Fetch all unionOf relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#unionOf ", "")
            for triple in triples:
                if triple[0] not in unique_classes:
                    # add class in subject position
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1

            # Fetch all intersectionOf relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#intersectionOf ", "")
            for triple in triples:
                if triple[0] not in unique_classes:
                    # add class in subject position
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1

            # Fetch all oneOf relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#oneOf ", "")
            for triple in triples:
                if triple[0] not in unique_classes:
                    # add class in subject position
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1

            # Fetch all onClass relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#onClass ", "")
            for triple in triples:
                if triple[2] not in unique_classes:
                    # add class in object position
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1

            # Fetch all complementOf relations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#complementOf ", "")
            for triple in triples:
                if triple[0] not in unique_classes:
                    # add class in subject position
                    unique_classes.add(triple[0])
                    undeclared_classes_count += 1
                if triple[2] not in unique_classes:
                    # add class in object position
                    unique_classes.add(triple[2])
                    undeclared_classes_count += 1

            # Fetch all Property declarations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property")
            for triple in triples:
                if triple[0] not in unique_properties:
                    unique_properties.add(triple[0])
                    declared_properties_count += 1

            # Fetch all Object Property declarations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2002/07/owl#ObjectProperty")
            for triple in triples:
                if triple[0] not in unique_properties:
                    unique_properties.add(triple[0])
                    declared_properties_count += 1

            # Fetch all DataType Property declarations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2002/07/owl#DataTypeProperty")
            for triple in triples:
                if triple[0] not in unique_properties:
                    unique_properties.add(triple[0])
                    declared_properties_count += 1

            # Fetch all triples
            (triples, cardinality) = document.search_triples("", "", "")
            for triple in triples:

                # Fetch all Properties
                if triple[1] not in unique_properties:
                    unique_properties.add(triple[1])
                    undeclared_properties_count += 1

                # Fetch all reused Individuals that are not declared
                if triple[0] not in declared_individuals and triple[0] not in unique_classes:
                    reused_individuals.add(triple[0])
                    reused_individuals_count += 1
                    if authoritative_dataset_URI not in triple[2] and "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in triple[1]:
                        linked_individuals_count += 1
                        linked_individuals.add(triple[0])

                # if object is a reused individial and not declared
                if triple[2] not in declared_individuals:
                    #o = urlparse()
                    if "http" in triple[2]:
                        if authoritative_dataset_URI not in triple[0] and not triple[0].startswith("_:") and "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in triple[1]:
                            linked_individuals_count += 1
                            linked_individuals.add(triple[0])

                # Fetch all Class Links in subject position
                if authoritative_dataset_URI not in triple[0] and triple[0] in unique_classes and "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in triple[1] :
                    #o = urlparse(triple[2])
                    #if o.netloc:
                    if "http" in triple[2]:
                        class_link_count += 1

                # Fetch all Class Links in object position
                if authoritative_dataset_URI not in triple[2] and triple[2] in unique_classes and "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in triple[1]:
                    class_link_count += 1

                # Fetch all Property Links where authoritative URI is in subject position
                if authoritative_dataset_URI in triple[0] and (
                        "http://www.w3.org/2000/01/rdf-schema#SubPropertyOf" in triple[
                    1] or "http://www.w3.org/2002/07/owl#propertyChainAxiom" in triple[
                            1]
                        or "http://www.w3.org/2002/07/owl#equivalentProperty" in triple[
                            1] or "http://www.w3.org/2002/07/owl#propertyDisjointWith" in triple[
                            1] or "http://www.w3.org/2002/07/owl#inverseOf" in triple[1]):
                    property_link_count += 1

                # Fetch all Property Links where authoritative URI is in object position
                if authoritative_dataset_URI in triple[2] and (
                        "http://www.w3.org/2000/01/rdf-schema#SubPropertyOf" in triple[
                    1] or "http://www.w3.org/2002/07/owl#onProperty" in triple[
                            1] or "http://www.w3.org/2002/07/owl#assertionProperty" in triple[
                            1] or "http://www.w3.org/2002/07/owl#equivalentProperty" in triple[
                            1] or "http://www.w3.org/2002/07/owl#propertyDisjointWith" in triple[
                            1] or "http://www.w3.org/2002/07/owl#inverseOf" in triple[1]):
                    property_link_count += 1

                # If external property used in subject of rdfs:range or rdfs:domain
                if authoritative_dataset_URI not in triple[0] and ("http://www.w3.org/2000/01/rdf-schema#domain" in triple[1] or "http://www.w3.org/2000/01/rdf-schema#range" in triple[1]):
                    property_link_count += 1


            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#sameAs", "")
            for triple in triples:
                if authoritative_dataset_URI not in triple[2]:
                    sameas_link_count += 1

            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2000/01/rdf-schema#seeAlso", "")
            for triple in triples:
                if authoritative_dataset_URI not in triple[2]:
                    seeAlso_link_count += 1

            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#differentFrom", "")
            for triple in triples:
                if authoritative_dataset_URI not in triple[2]:
                    differentFrom_link_count += 1

            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2002/07/owl#AllDifferent", "")
            for triple in triples:
                if authoritative_dataset_URI not in triple[2]:
                    allDifferent_link_count += 1

            # Fetch all SubProperty declarations
            (triples, cardinality) = document.search_triples("", "http://www.w3.org/2000/01/rdf-schema#SubPropertyOf", "")
            for triple in triples:
                if triple[0] not in unique_properties:
                    unique_properties.add(triple[0])
                    undeclared_properties_count += 1
                if triple[2] not in unique_properties:
                    unique_properties.add(triple[2])
                    undeclared_properties_count += 1

            # Initialise statistics dictionaries
            statistics["Declared Classes"] = declared_classes_count
            statistics["Undeclared Classes"] = undeclared_classes_count
            statistics["Declared Properties"] = declared_properties_count
            statistics["Undeclared Properties"] = undeclared_properties_count
            statistics["Unique Individuals"] = declared_individuals_count
            statistics["Reused Individuals"] = reused_individuals_count
            statistics["Linked Individuals"] = linked_individuals_count
            statistics["SameAs Links"] = sameas_link_count
            statistics["SeeAlso Links"] = seeAlso_link_count
            statistics["DifferentFrom Links"] = differentFrom_link_count
            statistics["AllDifferent Links"] = allDifferent_link_count
            statistics["Class Link Count"] = class_link_count
            statistics["Property Link Count"] = property_link_count
            statistics["Instance Typing Link Count"] = instanceTyping_link_count

            # Read in all classes retrieved from prefix.cc
            with open('unique_classes.csv') as csv_file:
                csv_reader = csv.reader(csv_file)
                for row in csv_reader:
                    all_classes.add(row[0])
                for unique_class in unique_classes:
                    if unique_class not in all_classes:
                        unregistered_classes.add(unique_class)
                csv_file.close()

            # Read in all properties retrieved from prefix.cc
            with open('unique_properties.csv') as csv_file:
                csv_reader = csv.reader(csv_file)
                for row in csv_reader:
                    all_properties.add(row[0])
                for unique_property in unique_classes:
                    if unique_property not in all_properties:
                        unregistered_properties.add(unique_property)
                csv_file.close()

            # Write statistics to file
            with open('statistics/statistics' + filename + '.csv', 'w') as csv_file:
                writer = csv.writer(csv_file)
                for key, value in statistics.items():
                    writer.writerow([key, value])
                csv_file.close()

            # Write unique_properties to file
            with open('statistics/unique_properties' + filename + '.csv', 'w') as csv_file:
                writer = csv.writer(csv_file)
                for val in unique_properties:
                    writer.writerow([val])
                csv_file.close()

            # Write unique_classes to file
            with open('statistics/unique_classes' + filename + '.csv', 'w') as csv_file:
                writer = csv.writer(csv_file)
                for val in unique_classes:
                    writer.writerow([val])
                csv_file.close()

            # Write unregistered_classes to file
            with open('statistics/unregistered_classes' + filename + '.csv', 'w') as csv_file:
                writer = csv.writer(csv_file)
                for val in unregistered_classes:
                    writer.writerow([val])
                csv_file.close()

            # Write unregistered_classes to file
            with open('statistics/unregistered_properties' + filename + '.csv', 'w') as csv_file:
                writer = csv.writer(csv_file)
                for val in unregistered_properties:
                    writer.writerow([val])
                csv_file.close()
        except Exception as e: print(e)
