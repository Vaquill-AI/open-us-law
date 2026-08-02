
from pydantic import BaseModel, Field, HttpUrl
from pydantic.types import conlist, Json
from pydantic import BaseModel, validator, ValidationError, model_validator, field_validator, field_serializer, model_serializer, computed_field, root_validator, ValidationError,ValidationInfo
import json
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import date, datetime, time, timedelta
from functools import wraps
import datetime
import re
import inspect




class NodeID(BaseModel):
    """
    Pydantic model to represent the hierarchical id for a Node of legislation.
    DISALLOWED CHARS: "/", "=",
    """
    raw_id: Optional[str]
    component_levels: List[str] = []
    component_classifiers: List[str] = []
    component_numbers: List[str] = []


    # Validate the provided raw_id is valid, and parse the components
    @model_validator(mode="after")
    def parse_components(self) -> 'NodeID':

        raw_id = self.raw_id
        if raw_id is None:
            # Allow for empty raw_id
            return self

        # Split on "/"
        component_levels = raw_id.split('/')
        component_classifiers = []
        component_numbers = []
        for i, level in enumerate(component_levels):
            # Skip the first three components, which are identifiers for the
            # jurisdiction, subjurisdiction, and the corpus
            if i == 0:
                component_classifiers.append("country")
                component_numbers.append(level)
                continue
            if i == 1:
                component_classifiers.append("jurisdiction")
                component_numbers.append(level)
                continue
            if i == 2:
                component_classifiers.append("corpus")
                component_numbers.append(level)
                continue
            if "=" not in level:
                raise ValueError(f"Invalid level format: {level}")
            classifier, number = level.split('=')

            # TODO: MAKE ALLOWED_LEVELS DYNAMIC FOR EACH JURISDICTION
            if classifier not in ALLOWED_LEVELS:
                raise ValueError(f"Invalid level classifier: {classifier}")
            component_classifiers.append(classifier)
            component_numbers.append(number)

        # Update the values dict to include the parsed components
        self.component_levels = component_levels
        self.component_classifiers = component_classifiers
        self.component_numbers = component_numbers
        return self


    # Property to get the current level classifier e.g.: SECTION, 10
    @property
    def current_level(self):
        """
        Property to get the current level_classifier and number as a tuple (level_classifier, number)
        Ex: (section, 10)
        """
        if self.component_levels:
            return self.component_classifiers[-1], self.component_numbers[-1]

        return None

    # Property to get the parent level, and number e.g.: PART, 214
    @property
    def parent_level(self):
        """
        Property to get the parent level_classifier and number as a tuple (level_classifier, number)
        Ex: (part, 214)
        """
        if len(self.component_levels) > 2:
            return self.component_classifiers[-2], self.component_numbers[-2]
        return None


    def pop_level(self) -> 'NodeID':
        """
        Remove the last component from the node_id, return updated NodeID
        """
        # Remove the last component from the node_id
        return NodeID(raw_id='/'.join(self.component_levels[:-1]))

    # Property to get the ID at any level, e.g TITLE
    def search_for_parent_level(self, level_classifier):
        """
        Function to find a parent NodeID by searching for a level_classifier.
        """
        # Find the component that starts with the level. Then return the raw_id from the start to the found component
        for i, component in enumerate(self.component_levels):
            if component.startswith(level_classifier):
                return '/'.join(self.component_levels[:i+1])
        return None

    def add_level(self, new_level: str, new_number: str) -> 'NodeID':
        """
        Creates a new node_id by appending a new level and its number to the current node_id.

        :param new_level: The new level to add (e.g., "SECTION").
        :param new_number: The number associated with the new level (e.g., "214.2").
        :return: A new NodeID instance with the updated node_id.
        """
        new_raw_id = f"{self.raw_id}/{new_level}={new_number}"
        return NodeID(raw_id=new_raw_id)

    def add_starter_level(self, new_level:str) -> 'NodeID':
        """
        Creates a new node_id by appending a new level to the current node_id. For use when
        creating a JURISDICTION, SUBJURISDICTION, or CORPUS level.
        """
        new_raw_id = f"{self.raw_id}/{new_level}"
        return NodeID(raw_id=new_raw_id)

    @model_serializer
    def serialize_node_id(self) -> str:
        return self.raw_id

# ===== Definition Models =====
class Definition(BaseModel):
    """
    Pydantic Model for holding a legal definition for a term.
    """
    definition: str = Field(..., description="Definition text.")
    subdefinitions: Optional[List['Definition']] = Field(default=None, description="List of subdefinitions. Tuple of (subdefinition paragraph id, subdefinition).")
    source_section_id: Optional[str] = Field(..., description="Section ID of the source definition. ie title=10/chapter=1/section=1.1. None if same as the current section.")
    source_paragraph_id: str = Field(..., description="Paragraph ID of the source definition. ie 1.1(a)(1)(i)")
    source_link: HttpUrl = Field(..., description="URL link to the exact paragraph of the definition.")
    is_subterm: Optional[bool] = Field(default=None, description="Is this definition a subterm of another definition?")

class IncorporatedTerms(BaseModel):
    """
    A DefinitionHub can incorporate terms from another DefinitionHub. This is a way to create these linked Terms/Definitions
    """
    import_source_link: Optional[str] = Field(default=None, description="URL link to the source of the incorporated definition(s).")
    import_source_corpus: Optional[str] = Field(default=None, description="Corpus/Document of the source definition(s). None if same as the current corpus.")
    import_source_id: Optional[str] = Field(default=None, description="Structured identifier for the source of the incorporated definition(s).")
    terms: Optional[List[str]] = Field(default=None, description="List of terms incorporated from the source. If None, all terms/definitions are incorporated.")

class DefinitionHub(BaseModel):
    """
    A Way to attach a hub of Definitions to a given Node. Includes terms defined locally, terms incorporated from elsewhere, as well as the scope of where these terms apply.
    """
    local_terms: Dict[str, Definition] = Field(default={}, description="Dictionary of locally defined terms and definitions.")
    incorporated_terms: Optional[List[IncorporatedTerms]] = Field(default_factory=list, description="List of imported terms and definitions, incorporated by references. List of unique sources.")
    scope: Optional[str] = Field(default=None, description="Scope of the local definitions combined with any incroporated definitions. Where should these new definitions be applied?")
    scope_ids: Optional[List[str]] = Field(default=None, description="Structured identifiers for the targeted scopes of extracted definitions corresponding to the database ids")


# ===== Reference Models =====
class Reference(BaseModel):
    """
    TODO
    """

    text: str = Field(..., description="Extracted text of the reference.")
    placeholder: Optional[str] = Field(default=None, description="Placeholder for the reference. ex: [*i*]")
    corpus: Optional[str] = Field(default=None, description="Corpus/Document of the target regulation. None if same as the current corpus.")
    id: Optional[str] = Field(default=None, description="Structured identifier for the target. ")
    paragraph_id : Optional[str] = Field(default=None, description="Paragraph ID of the target reference. ie 1.1(a)(1)(i)")

class ReferenceHub(BaseModel):
    # Key Should be The Link
    references: Optional[Dict[str, Reference]] = Field(default={}, description="Dictionary of references.")

    def combine(self, other: 'ReferenceHub') -> 'ReferenceHub':
        """
        Combines two ReferenceHub instances into a single ReferenceHub instance.
        """
        combined_references = {**self.references, **other.references}
        return ReferenceHub(references=combined_references)

# ===== Node Text Models =====
class Paragraph(BaseModel):
    index: int = Field(..., description="Index of the paragraph.")
    parent: str = Field(default="ROOT", description="Parent paragraph ID.")
    children: Optional[List[str]] = Field(default_factory=list, description="List of child paragraph IDs.")

    text: str = Field(..., description="Text content of the paragraph.")
    reference_hub: Optional[ReferenceHub] = Field(default=None, description="References associated with the paragraph.")
    classification: Optional[str] = Field(default=None, description="Classification of the paragraph. ex: Definition")
    topic: Optional[str] = Field(default=None, description="Topic of the paragraph. ex: Scope")


class NodeText(BaseModel):
    """
    Paragraphs - Key: "paragraph_id", value: Paragraph
    """
    paragraphs: Dict[str, Paragraph] = Field(default={})

    # Return True if the paragaph already exists
    def add_paragraph(self, text: str, paragraph_id: Optional[str]=None, parent: Optional[str]=None, children: Optional[List[str]]=None, reference_hub: Optional[ReferenceHub]=None, classification: Optional[str]=None, topic: Optional[str]=None) -> bool:
        """
        Add a new paragraph to NodeText with a paragraph_id.
        """
        if paragraph_id is None:
            paragraph_id = f"#p-{len(self.paragraphs)}"
        if parent is None:
            parent="ROOT"
        paragraph = Paragraph(index=len(self.paragraphs), parent=parent, children=children, text=text, reference_hub=reference_hub, classification=classification, topic=topic )
        # If the paragraph already exists
        if paragraph_id in self.paragraphs:
            previous_index = self.paragraphs[paragraph_id].index

            # If the paragraph already exists at the same index, combine the paragraphs
            if previous_index == paragraph.index-1:

                self.paragraphs[paragraph_id].text += f"\n{paragraph.text}"
                # Optionally override the classification and topic
                if paragraph.classification:
                    self.paragraphs[paragraph_id].classification = paragraph.classification
                if paragraph.topic:
                    self.paragraphs[paragraph_id].topic = paragraph.topic
                # Optionally combine the new references
                if paragraph.references:
                    # If the paragraph already has references, combine the new references with the existing references
                    if self.paragraphs[paragraph_id].reference_hub:
                        self.paragraphs[paragraph_id].reference_hub = self.paragraphs[paragraph_id].reference_hub.combine(paragraph.reference_hub)
                    # If the paragraph does not have references, set the references to the new references
                    else:
                        self.paragraphs[paragraph_id].reference_hub = paragraph.reference_hub

                #raise ValueError(f"Debugging! New Text: {self.paragraphs[paragraph_id].text}")
                # with open("combined_paragraphs.txt", "a") as file:
                #     file.write(f"{paragraph_id}\n")
            else:
                # If the paragraph already exists at a different index, increment the index and add the paragraph

                # TODO: REVIEW THIS LOGIC! HOW SHOULD WE HANDLE COPIES?
                new_id = increment_copy_number(paragraph_id)
                if paragraph.parent == paragraph_id:
                    paragraph.parent = new_id
                #print(f"Paragraph already exists at index {previous_index}. Incrementing index and adding new paragraph with id {new_id}.")
                self.paragraphs[new_id] = paragraph

                # with open("incremented_paragraphs.txt", "a") as file:
                #     file.write(f"{new_id}\n")
                #raise ValueError(f"Debugging! New key (id): {new_id}")
                return new_id

            return paragraph_id


        self.paragraphs[paragraph_id] = paragraph
        return paragraph_id

    def pop(self) -> Paragraph:
        """
        Pop the last Paragraph by Index
        """
        highest_index = -1
        highest_paragraph_id=""
        # This is dumb and I should find a better way to do this
        for paragraph_id,paragraph in self.paragraphs.items():
            if paragraph.index > highest_index:
                highest_index = paragraph.index
                highest_paragraph_id = paragraph_id

        popped_paragraph = self.paragraphs[highest_paragraph_id]
        del self.paragraphs[highest_paragraph_id]

        return popped_paragraph


    def to_list_paragraph(self) -> List[Paragraph]:
        """
        Return NodeText as a List of Paragraph Pydantic Models
        """
        sorted_paragraphs = sorted(self.paragraphs.values(), key=lambda p: p.index)
        return sorted_paragraphs

    def to_list_text(self) -> List[str]:
        """
        Return NodeText as a list of string
        """
        sorted_paragraphs = sorted(self.paragraphs.values(), key=lambda p: p.index)
        result_text = [paragraph.text for paragraph in sorted_paragraphs]
        return result_text

    # TODO: THIS NEEDS TESTING, NOT SURE IT STILL WORKS WITH UPDATED PARAGRAPH MODEL LOGIC!
    def extrapolate_children_from_parents(self):
        """
        Given that each Paragraph knows it's own parent, extrapolate each nested paragraphs children
        """
        # Initialize a mapping to hold lists of children for each parent ID
        children_map = {}
        for id, paragraph in self.paragraphs.items():
            parent_id = paragraph.parent
            if parent_id not in children_map:
                children_map[parent_id] = []
            if id != parent_id:
                children_map[parent_id].append(id)
        #print(f"Children Map: {children_map}")
        # Use the children_map to set the children for each paragraph
        #print(f"Paragraphs: {self.paragraphs}")
        for id, paragraph in self.paragraphs.items():
            #print(id)
            if id in children_map:


                if id == paragraph.parent:
                    paragraph.parent = "ROOT"




                elif paragraph.parent in children_map[id]:
                     children_map[id] = children_map[id].remove(paragraph.parent)



                paragraph.children = children_map[id]

        return self

    def to_tree(self) -> Dict[str, Any]:
        """
        I can't remember what this function does.
        """

        paragraphs = self.paragraphs
        root_paragraph_id = self.root_paragraph_id

        # Initialize a mapping to hold lists of children for each parent ID


        def build_node(node_id: str) -> Dict[str, Any]:
            node = paragraphs[node_id]
            # Use the children_map to get the children IDs for this node

            children = [build_node(child_id) for child_id in node.children]
            # Sort children by index before adding to the node

            return {"text": node.text, "children": children}

        # Assuming root_paragraph_id is the ID of the root node. If it's actually the parent ID shared by root nodes,
        # find all root nodes having the root_paragraph_id as their parent.
        root_nodes = [id for id, paragraph in paragraphs.items() if paragraph.parent == root_paragraph_id]
        sorted_root_nodes = sorted(root_nodes, key=lambda root_id: paragraphs[root_id].index)
        sorted_root_children = [build_node(root) for root in sorted_root_nodes]

        # Create a "true" root node that collects all roots as its children, sorted by index
        true_root = {
            "text": "Root Default",
            "children": sorted_root_children
        }

        return true_root


# ===== Addendum Models =====
class AddendumType(BaseModel):
    """
    A Pydantic Model which concerns Addendums to sections of legislation. There are 3 possible types of addendums: about history, about the source of legislation, or about the authority of legislation.
    """

    type: str = Field(default="history", description="Type of the addendum. ex: history, source, authority, etc.")
    text: str = Field(..., description="Text content of the addendum.")
    prefix: Optional[str] = Field(default=None, description="Prefix for the addendum.")
    reference_hub: Optional[ReferenceHub] = Field(default=None, description="References associated with the addendum.")

class Addendum(BaseModel):
    """
    Holds all addendum information for a given section of legislation. ** Note: Could be revisited to alter AddendumType and allow for more flexibility**
    """
    source: Optional[AddendumType] = Field(default=None, description="Source addendum.")
    authority: Optional[AddendumType] = Field(default=None, description="Authority addendum.")
    history: Optional[AddendumType] = Field(default=None, description="History addendum.")

    metadata: Optional[Dict] = Field(default=None, description="Metadata for the addendum in JSON format.")


    def get(self) -> str:
        """
        Return a string representation of all possible addendums
        """
        result = []
        if self.source:
            result.append(self.source.text)

        if self.history:
            result.append(self.history.text)

        if self.authority:
            result.append(self.authority.text)

        result = '\n'.join(result)
        return result

class Node(BaseModel):
    """
    A Pydantic model represented a single Node of legislation. Two node types: 1. Structure, structures other pieces of legislation, does not contain actual text. 2. Content: contains actual written text of legislation.
    """
    # Fields Used For Identification
    id: Union[NodeID, str] = Field(..., description="Primary key of the node.", repr=True)  # Assuming NodeID is a str for simplification
    citation: Optional[str] = Field(default=None, description="Citation information for the node.", repr=True)
    link: Optional[HttpUrl] = Field(default=None, description="URL link associated with the node.", repr=True)

    # Core Fields
    status: Optional[str] = None
    node_type: str = Field(..., description="Type of the node. Must be 'content' or 'structure'", repr=True)
    top_level_title: Optional[str] = None
    level_classifier: str = Field(..., description="Classifier for the node's level.")
    number: Optional[str] = None
    node_name: Optional[str] = None

    node_text: Optional[NodeText] = Field(default=None, description="Text content associated with the node.")
    definition_hub: Optional[DefinitionHub] = Field(default=None, description="Definitions associated with the node in JSON format.")
    core_metadata: Optional[Dict] = Field(default=None, description="Metadata for the node in JSON format.")
    processing: Optional[Dict] = Field(default=None, description="Processing information for the node in JSON format.")

    # Addendum Fields
    addendum: Optional[Addendum] = Field(default=None, description="Addendum associated with the node in JSON format.")

    # Additional Fields
    summary: Optional[str] = None
    hyde: Optional[List[str]] = None

    agencies: Optional[List[str]] = Field(default=None, description="A list of government agencies who manage this node.")

    # Node/Graph Traversal Fields
    parent: str = Field(default=None, description="Parent node ID.")
    direct_children: Optional[List[str]] = None
    incoming_references: Optional[ReferenceHub] = Field(default=None, description="Incoming references in Dictionary format.")

    # Embedding Fields
    text_embedding: Optional[List[float]] = Field(default=None, description="Text embedding vector.")
    summary_embedding: Optional[List[float]] = Field(default=None, description="Summary embedding vector.")
    hyde_embedding: Optional[List[float]] = Field(default=None, description="Hyde embedding vector.")
    name_embedding: Optional[List[float]] = Field(default=None, description="Embedding on the NAME of legislation (possibly including parent names)")

    # Metadata Fields
    date_created: Optional[datetime.datetime] = Field(default_factory=datetime.datetime.now, description="Date the node was created.")
    date_modified: Optional[datetime.datetime] = Field(default_factory=datetime.datetime.now, description="Date the node was last modified (From Python)")

    #TODO: REWORK THIS TO BE BEST PRACTICES FOR NEWER PYDANTIC VERSIONS
    @field_validator('id', mode="before")
    @classmethod
    def validate_nodeID(cls, v: Union[str, NodeID], info: ValidationInfo) -> NodeID:
        if isinstance(v, str):
            return NodeID(raw_id=v)
        return v

    @property
    def node_id(self):
        """
        String accessor alias for node's 'id' field, instead of pydantic NodeID.
        """
        return self.id.raw_id



# ===== API Models =====
class ChatMessage(BaseModel):
    role: str
    content: str

class APIParameters(BaseModel):
    # Common parameters
    vendor: str
    model: str
    messages: List[ChatMessage]
    temperature: float = Field(1, le=1, gt=0)
    top_p: Optional[float] = Field(1, le=1, gt=0)
    frequency_penalty: float = Field(0, le=1, ge=0)
    max_tokens: Optional[int] = None  # Anthropic specific
    stream: Optional[bool] = False

    # OpenAI Specific
    response_format: Optional[Dict[str, Any]] = None
    presence_penalty: float = Field(0, le=1, ge=0)
    # Instructor specific
    response_model: Optional[str] = None  # Instructor specific
    max_retries: Optional[int] = Field(default=1)  # Instructor specific

    # Metadata for cost analysis and logging
    calling_function: Optional[str] = None


    @root_validator(pre=True)
    def set_calling_function(cls, values):
        # If calling_function is not already set, determine it dynamically
        if 'calling_function' not in values or values['calling_function'] is None:
            values['calling_function'] = inspect.stack()[1].function
        return values

class APIUsage(BaseModel):
    model: str = Field(..., description="The LLM model used for the request")
    vendor: str = Field(..., description="The vendor used for the request")
    response_id: Optional[str] = Field(default=None, description="Unique identifier for the record")

    input_tokens: Optional[int] = Field(..., description="Number of prompt tokens")
    output_tokens: Optional[int] = Field(..., description="Number of completion tokens")
    total_tokens: Optional[int] = Field(..., description="Total number of tokens used in the request")

    input_cost: Optional[float] = None
    output_cost: Optional[float] = None
    total_cost: Optional[float] = None

    status: Optional[str] = Field(None, description="Status of the API request")
    error_message: Optional[str] = Field(None, description="Error message if the request failed")
    duration: Optional[float] = Field(None, description="Duration of the API request in seconds")

    calling_function: str = Field(..., description="Name of the Python function that initiated the request")
    api_key_name: Optional[str] = Field(None, description="Name of the API key used for the request")

    timestamp: datetime.datetime = Field(default=None, description="Timestamp of the API request")

    @model_validator(mode='after')
    def compute_cost(self) -> 'APIUsage':
        # Don't recompute if the cost is already set
        if self.total_cost is not None:
            return self
        # Open the JSON file and load pricing data
        with open("api_pricing.json", "r") as file:
            pricing_data = json.load(file)

        # Access the vendor and model specific pricing information
        try:
            model_pricing = pricing_data[self.vendor][self.model]
        except KeyError:
            raise ValueError(f"Pricing data not found for model {self.model} and vendor {self.vendor}")

        # Calculate costs
        self.input_cost = (self.input_tokens / 1e6) * float(model_pricing["input_price"])
        self.output_cost = (self.output_tokens / 1e6) * float(model_pricing["output_price"])
        self.total_cost = self.input_cost + self.output_cost
        return self

    def insert(self, user: str):
        from src.utils import utilityFunctions as util
        util.pydantic_insert("api_usage", [self], include=None)




def analyze_partial_link(link: str, user: str) -> Tuple[str, str]:
    from src.utils import utilityFunctions as util
    #Example link: https://www.ecfr.gov/current/title-24/section-891.105
    #print(f"OG Link: {link}")
    # Split the link into parts
    parts = link.split("/")
    if len(parts) < 5:
        return None, None
    #print(parts)
    # Extract the corpus, title, and section
    corpus = parts[2]
    title_level = parts[4]
    top_level_title = title_level.split("-")[-1]
    last_level = parts[-1]
    remaining_levels = parts[5:]
    #print(remaining_levels)

    statements = []

    for i, level in enumerate(remaining_levels):
        level_split = level.split("-")
        level_classifier = level_split.pop(0)
        if level_classifier == "subject":
            level_classifier += "-" + level_split.pop(0)
        level_number = "-".join(level_split).replace("%20", "-")
        statements.append((level_classifier, level_number))

    #print(f"SQL Statement: {statements}")
    if len(statements) == 0:
        full_statement = f"WHERE top_level_title = '{top_level_title}' AND level_classifier = 'title';"
        sql_select = f"SELECT id, link FROM us_federal_ecfr {full_statement}"
        try:
            row = util.regular_select(sql_select)
            correct_link = row[0]['link']

        except:
            pass



    while statements:
        full_statement = f"WHERE top_level_title = '{top_level_title}';"
        for statement in statements:
            level_classifier, level_number = statement
            where_statement = build_where_statement(level_classifier, level_number)
            full_statement = f"{full_statement[:-1]} AND {where_statement};"
            #print(f"full_statement: {full_statement}")
        full_statement = full_statement[:-1]
        extra_statement = f" AND level_classifier = '{level_classifier}' AND number = '{level_number}';"
        #print(f"final_statement: {full_statement}")

        #print(f"full_statement: {full_statement}")
        sql_select = f"SELECT id, link FROM us_federal_ecfr {full_statement}{extra_statement}"
        try:
            row = util.regular_select(sql_select)
            correct_link = row[0]['link']
            break
        except:
            if level_classifier == "section":
                level_number += "0"
                extra_statement = f" AND level_classifier = '{level_classifier}' AND number = '{level_number}';"
                sql_select = f"SELECT id, link FROM us_federal_ecfr {full_statement}{extra_statement}"
                try:
                    row = util.regular_select(sql_select)
                    correct_link = row[0]['link']
                    break
                except:
                    pass

            statements.pop()
            #print("Statement Failed")
            #print(statements)



    try:
        correct_id = row[0]['id']
    except:
        return None, None
    #print(f"Correct Link: {correct_link}, Correct ID: {correct_id}")

    return correct_link, correct_id

def build_where_statement(level_classifier:str, level_number:str) -> str:

    return f"id ILIKE '%{level_classifier}={level_number}%'"

def increment_copy_number(paragraph_id: str) -> str:
    if "-copy-" in paragraph_id:
        base_id, copy_number = paragraph_id.rsplit("-copy-", 1)
        new_copy_number = int(copy_number) + 1
        new_id = f"{base_id}-copy-{new_copy_number}"
    else:
        new_id = f"{paragraph_id}-copy-1"
    return new_id




def fetch_definitions(user: str, node_id: Optional[str] = None, link: Optional[str] = None ):
    """
    This function fetches definitions from a PostgreSQL database for a given node_id or link.
    Each node in the database can optionally have a child definition hub, which contains relevant terms and definitions.
    If a definition hub exists, it can have its own definitions called "local_definitions", and also can have "incorporated_definitions".
    Incorporated_definitions means that another specific definition_hub applies.

    Parameters:
    user (str): The user who is executing the function.
    node_id (Optional[str]): The id of the node to fetch definitions for. If not provided, link must be provided.
    link (Optional[str]): The link of the node to fetch definitions for. If not provided, node_id must be provided.

    Returns:
    all_definitions (list): A list of tuples, where each tuple contains a node id and its corresponding definitions dictionary.

    Raises:
    ValueError: If neither node_id nor link is provided.
    """
    from src.utils import utilityFunctions as util

    if node_id is None and link is None:
        raise ValueError("Either node_id or link must be provided to fetch_definitions.")

    if node_id:
        new_id = NodeID(raw_id=node_id)
    else:
        sql_select = f"SELECT id FROM us_federal_ecfr WHERE link = '{link}';"
        row = util.pydantic_select(sql_select, classType=None)
        new_id = NodeID(raw_id=row[0]['id'])


    all_definitions = []
    counter = 0

    # Loop until the raw_id of new_id is 'us/federal/ecfr' or counter reaches 10
    while new_id.raw_id != 'us/federal/ecfr' and counter < 10:
        # Construct the id of the definition hub
        definition_id = f"{new_id.raw_id}/hub=definitions"
        print(f"Fetching definitions for id: {definition_id}")
        # Construct the SQL query to fetch the local and incorporated definitions from the definition hub
        select_definition = f"SELECT definitions ->> 'local_definitions' as def_dict, definitions ->> 'incorporated_definitions' as inc FROM us_federal_ecfr WHERE id = '{definition_id}';"
        try:
            # Execute the SQL query and fetch the results
            data = util.pydantic_select(select_definition, classType=None)
            # Parse the local and incorporated definitions from the results
            def_dict = json.loads(data[0]['def_dict'])
            print(f"  - Starting with {len(def_dict)} definitions.")
            try:
                inc_dict = json.loads(data[0]['inc'])

            except:
                print("  - No incorporated definitions found.")
                inc_dict = []

            # Loop through each incorporated definition
            for inc_obj in inc_dict:
                # If the incorporated definition has an import_source_corpus, skip it
                if inc_obj.get('import_source_corpus') is not None:
                    continue
                # Get the id of the incorporated definition
                inc_id = inc_obj['import_source_id']
                print(f"  - Fetching definitions for incorporated id: {inc_id}")
                try:
                    # Construct the SQL query to fetch the scope_ids from the incorporated definition
                    select_inc_scope = f"SELECT definitions ->> 'scope_ids' as scope_list FROM us_federal_ecfr WHERE id = '{inc_id}';"
                    # Execute the SQL query and fetch the results
                    data = util.pydantic_select(select_inc_scope, classType=None)
                    # Parse the scope_ids from the results
                    scope_list = json.loads(data[0]['scope_list'])
                    # Construct the id of the scope definition hub
                    scope_id = f"{scope_list[0]}/hub=definitions"

                    # Construct the SQL query to fetch the local definitions from the scope definition hub
                    select_inc_definition = f"SELECT definitions ->> 'local_definitions' as inc_def_dict FROM us_federal_ecfr WHERE id = '{scope_id}';"
                    # Execute the SQL query and fetch the results
                    data2 = util.pydantic_select(select_inc_definition, classType=None)
                    # Parse the local definitions from the results
                    inc_def_dict = json.loads(data2[0]['inc_def_dict'])

                    print(f"    * Fetched {len(inc_def_dict)} definitions")

                    # Loop through each key in the local definitions
                    for key in inc_def_dict:
                        # If the source_section of the definition is None or equals to inc_id, add it to def_dict
                        if inc_def_dict[key].get('source_section') is None or inc_def_dict[key]['source_section'] == inc_id:
                            def_dict[key] = inc_def_dict[key]

                except Exception as err:
                    print(f"    * No definition hub found")

            # Add the node id and its corresponding definitions to all_definitions
            all_definitions.append((new_id.raw_id, def_dict))
        except Exception as err:
            print(f"  - No definition hub found.")
            print(f"  - Error: {err}")
            #print(err)
        finally:
            new_id = new_id.pop_level()
            counter += 1




    return all_definitions


def main():
    from src.utils import utilityFunctions as util
    # test_link = "https://www.ecfr.gov/current/title-40/part-205/subpart-s"
    # analyze_partial_link(test_link, "will2")
    test_id = "us/federal/ecfr/title=7/subtitle=B/chapter=XI/part=1219/subpart=A/subject-group=ECFR70215de6cdda424/section=1219.54"
    sql_select = f"SELECT * FROM us_federal_ecfr WHERE id = '{test_id}';"
    row: Node = util.pydantic_select(sql_select, classType=Node, user="will2")[0]
    all_definitions = fetch_definitions("will2", node_id=test_id)


    definition_dict = all_definitions[0][1]
    with open("test_definitions.json", "w") as file:
        json.dump(definition_dict, file, indent=4)
    file.close()
    node_text = row.node_text.to_list_text()

    print(f"Node Text: {node_text}")
    filtered_definitions = filter_definitions_from_node_text(node_text, definition_dict)

    print(f"Filtered Definitions: {filtered_definitions.keys()}")



def filter_definitions_from_node_text(node_text: List[str], definitions: Dict[str, Definition]) -> Dict[str, Definition]:

    """
    This function filters a dictionary of definitions based on a list of text paragraphs.
    If a term is found in the node text, its definition is added to the filtered definitions.

    Parameters:
    node_text (List[str]): A list of text paragraphs.
    definitions (Dict[str, Definition]): A dictionary of definitions.

    Returns:
    filtered_definitions (Dict[str, Definition]): A dictionary of filtered definitions.
    """
    full_text = " ".join(node_text)
    filtered_definitions = {}
    for key, definition in definitions.items():
        # Search for the term in the full text, but only if it's a whole word
        if re.search(rf"\b{key}\b", full_text, re.IGNORECASE):
            filtered_definitions[key] = definition

    return filtered_definitions


def filter_definitions_from_node_text_p(node_text: List[Paragraph], definitions: Dict[str, Definition]) -> Dict[str, Definition]:

    """
    This function filters a dictionary of definitions based on a list of text paragraphs.
    If a term is found in the node text, its definition is added to the filtered definitions.

    Parameters:
    node_text (List[Paragraph]): A list of paragraphs as Pydantic models.
    definitions (Dict[str, Definition]): A dictionary of definitions.

    Returns:
    filtered_definitions (Dict[str, Definition]): A dictionary of filtered definitions.
    """
    full_text = ""
    for paragraph in node_text:
        full_text += paragraph.text + " "

    filtered_definitions = {}
    for key, definition in definitions.items():
        # Search for the term in the full text, but only if it's a whole word
        if re.search(rf"\b{key}\b", full_text, re.IGNORECASE):
            filtered_definitions[key] = definition

    return filtered_definitions

ALLOWED_LEVELS = [
    "title",
    "subtitle",
    "code",
    "part",
    "subpart",
    "division",
    "subdivision",
    "article",
    "subarticle",
    "chapter",
    "subchapter",
    "subject-group",
    "section",
    "appendix",
    "hub",
    "act"
]

if __name__ == "__main__":
    main()
