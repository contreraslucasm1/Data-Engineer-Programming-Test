"""
DE: Lucas Contreras
Etl class used in DAG: uniprot_etl
config:

{"url":"https://rest.uniprot.org/uniprotkb/Q9Y261.xml"}

OR

{"url":"https://rest.uniprot.org/uniprotkb/A0A1B0GTW7.xml"}

"""
import requests
from pprint import pprint
from bs4 import BeautifulSoup
from lxml import etree
from neo4j import GraphDatabase


class uniprot_etl:

    def __init__(self, uri, user, password, url):
        """Init method
        :param uri: Connection uri of the neo4j database
        :param user: username
        :param password: password
        :param url: input url of the xml to process
        :return: A succesfull message
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.content=self._validate_xml(url)
        self.url=url

    def close(self):
        """Close the neo4j connection"""
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_node(self, node):
        """
        Funtion to create a node
        :param driver: The neo4j driver object.
        :param node: A dictionary containing label and the properties of the node.
        :example:{'label': 'Protein', 'data': {'id': 'Q9Y261', 'name': 'FOXA2_HUMAN'}}
        :return: A succesfull message
        """
        with self.driver.session(database="neo4j") as session:
            props_str = ', '.join([f'{key}: ${key}' for key in node["properties"].keys()])
            query = f'MERGE (n:{node["label"]} {{ {props_str} }}) RETURN n'

            # Run the query with the provided node_dict as parameters
            result = session.run(query, node["properties"])

            # Extract the created node from the result and return it
            result.single()[0]
            return f'Node {node["label"]} has been created Sucessfully'

    def create_relationship(self, node1, node2, relationship):
        """
        Funtion to create a relation node1->node2
        :param node1,node2: A dictionary containing label and the properties of the node previously created.
            :example:{'label': 'Protein', 'properties': {'id': 'Q9Y261', 'name': 'FOXA2_HUMAN'}}
        :param relationship: A dictionary containing label and the properties of the relation
            :example{'label': 'FROM_GENE', 'properties': {'status': 'primary'}}
        :return: A succesfull message
        """
        with self.driver.session(database="neo4j") as session:
            props_str_node1 = ' AND '.join([f'n1.{key}= $n1_{key}' for key in node1["properties"].keys()])
            props_str_node2 = ' AND '.join([f'n2.{key}= $n2_{key}' for key in node2["properties"].keys()])
            props_str_relation = ', '.join([f'r.{key}= $r_{key}' for key in relationship.get("properties", {}).keys()])
            query = f"""
                MATCH
                  (n1:{node1['label']}),
                  (n2:{node2['label']})
                WHERE {props_str_node1} AND {props_str_node2}
                MERGE (n1)-[r:{relationship["label"]}]->(n2)
                {f"SET {props_str_relation}" if props_str_relation else ''}
                RETURN r.label
                """
            format_dict = {}
            for key, value in node1["properties"].items():
                format_dict[f'n1_{key}'] = value
            for key, value in node2["properties"].items():
                format_dict[f'n2_{key}'] = value
            for key, value in relationship.get("properties", {}).items():
                format_dict[f'r_{key}'] = value
            session.run(query, format_dict)

            return f'Relationship {relationship["label"]} has been created Sucessfully'

    def run_query(self, query):
        with self.driver.session() as session:
            result = session.run(query)
            return result

    def find_protein(self, protein_id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_protein, protein_id)
            for row in result:
                pprint("Found protein: {row}".format(row=row))

    @staticmethod
    def _find_and_return_protein(tx, protein_id):
        query = (
            "MATCH (p:Protein) "
            "WHERE p.id = $protein_id "
            "RETURN p.name AS name"
        )
        result = tx.run(query, protein_id=protein_id)
        return [row["name"] for row in result]

    @staticmethod
    def _validate_xml(url):
        """
        This function its a validator of the xmls
        :param url: url of the xml file
        """
        # download and parse the XML file
        xml_content = requests.get(url).content
        root = etree.fromstring(xml_content)
        # URL of the XSD schema file
        xsd_url = 'https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot.xsd'
        # download and parse the XSD schema file
        xsd_content = requests.get(xsd_url).content
        schema_root = etree.fromstring(xsd_content)
        # create a schema object
        schema = etree.XMLSchema(schema_root)
        # validate the XML against the schema
        if schema.validate(root):
            pprint("XML is valid against the schema.")
            return(xml_content)
        else:
            raise ValueError('XML is not valid against the schema. Please send a valid uniprot XML')

    def delete_nodes(self):
        with self.driver.session(database="neo4j") as session:
            query = (
                "MATCH (n:Protein)"
                "DETACH DELETE n"
            )
            result = tx.run(query)

    def main(self):
        """main funtion"""
        pprint(f'url get: {self.url}')
        soup = BeautifulSoup(self.content, 'xml')
        entry = soup.find("entry")
        # CREATE PROTEIN NODE
        node_protein = {"label": "Protein",
                        "properties": {"id": entry.find("accession").text, "name": entry.find("name").text}}
        self.create_node(node_protein)
        # CREATE GENE NODES AND RELATIONSHIP FROM_GENE
        for g in entry.find("gene").find_all("name"):
            node_gene = {"label": "Gene", "properties": {"name": g.string}}
            self.create_node(node_gene)
            node_relationship = {'label': 'FROM_GENE', 'properties': {'status': g['type']}}
            self.create_relationship(node_protein, node_gene, node_relationship)
        # CREATE Features NODES AND RELATIONSHIP HAS_FEATURE
        for feature_elem in entry.find_all("feature", type="modified residue"):
            description = feature_elem.get('description')
            type_name = feature_elem.get('type')
            begin_position = feature_elem.find('location').find('position').get('position')
            node_feature = {"label": "Feature", "properties": {"name": description, "type": type_name}}
            self.create_node(node_feature)
            node_relationship = {'label': 'HAS_FEATURE', 'properties': {'position': begin_position}}
            self.create_relationship(node_protein, node_feature, node_relationship)
        # CREATE FullName NODES AND RELATIONSHIP WITH HAS_FULL_NAME
        if entry.find("protein").find("recommendedName").find("fullName").text:
            node_fullname = {"label": "FullName",
                             "properties": {"name": entry.find("protein").find("recommendedName").find("fullName").text}}
            self.create_node(node_fullname)
            node_relationship = {'label': 'HAS_FULL_NAME'}
            self.create_relationship(node_protein, node_fullname, node_relationship)
        # CREATE Organism NODES AND RELATIONSHIP WITH IN_ORGANISM
        for organism in entry.find_all("organism"):
            node_organism = {"label": "Organism", "properties": {"name": organism.find('name', {'type': 'scientific'}).string,
                                                           "taxonomy_id": organism.find("dbReference")['id']}}
            self.create_node(node_organism)
            node_relationship = {'label': 'IN_ORGANISM'}
            self.create_relationship(node_protein, node_organism, node_relationship)
            for taxon in [taxon.text for taxon in organism.find_all('taxon')]:
                # CREATE Taxo NODES AND RELATIONSHIP WITH FROM_TAXO
                node_taxon = {"label": "Taxon", "properties": {"name": taxon}}
                self.create_node(node_taxon)
                node_relationship = {'label': 'FROM_TAXON'}
                self.create_relationship(node_organism, node_taxon, node_relationship)
        # CREATE Reference NODES AND RELATIONSHIP WITH HAS_REFERENCE
        for ref in entry.find_all("reference"):
            citation = ref.find('citation')
            node_reference = {"label": "Reference", "properties": {"name": citation.title.get_text(),
                                                             "citation_year": citation['date'].split('-')[0]}}
            self.create_node(node_reference)
            node_relationship = {'label': 'HAS_REFERENCE'}
            self.create_relationship(node_protein, node_reference, node_relationship)
            for person in [person['name'] for person in citation.authorList.find_all('person')]:
                node_author = {"label": "Author", "properties": {"name": person}}
            self.create_node(node_author)
            node_relationship = {'label': 'HAS_AUTHOR'}
            self.create_relationship(node_reference, node_author, node_relationship)
            for scope in [scope.text for scope in ref.find_all('scope')]:
                node_scope = {"label": "Scope", "properties": {"name": scope}}
            self.create_node(node_scope)
            node_relationship = {'label': 'HAS_SCOPE'}
            self.create_relationship(node_reference, node_scope, node_relationship)
        self.close()