
def main():
    insert_jurisdiction_and_corpus_node()
    with open(f"{DIR}/data/top_level_titles.txt") as text_file:
        for i, line in enumerate(text_file, start=1):  # Start enumeration from 1
            # if i < 8:
            #     continue
            url = line
            if ("title" in url):
                parts = url.split('/')
                number_or_string_after_statutes = parts[parts.index('statutes') + 1] if 'statutes' in parts else None

                top_level_title = number_or_string_after_statutes
                top_level_title = top_level_title
                scrape_per_title(url, top_level_title, "me/statutes/")
            else:
                continue

## TITLE -> CHAPTER -> SECTION
## TITLE -> PART -> SUBPART -> CHAPTER -> SECTION
## TITLE -> PART -> CHAPTER -> SECTION
## TITLE -> PART -> CHAPTER -> SUBCHAPTER -> SECTION
## TITLE -> ARTICLE -> PART -> SECTION
## TITLE -> ARTICLE -> SECTION
## TITLE -> SUBTITLE -> CHAPTER -> SECTION
## TITLE -> SUBTITLE -> PART -> SECTION


def scrape_per_title(url, top_level_title, node_parent):
    response = urllib.request.urlopen(url)
    data = response.read()      # a `bytes` object
    text = data.decode('utf-8') # a `str`;
    soup = BeautifulSoup(text)


    title_container = soup.find("div", class_="title_toc MRSTitle_toclist col-sm-10")
    title_name = title_container.find("div", class_="title_heading")
    title_node_name = title_name.get_text()
    title_node_number = title_node_name.split(" ")
    title_node_number = title_node_number[1].replace(":","")
    top_level_title = title_node_number
    node_type = "structure"
    node_level_classifier = "TITLE"
    node_id = f"{node_parent}TITLE={title_node_number}/"
    node_data = (node_id, top_level_title, node_type, node_level_classifier, None, None, None, url, None, title_node_name, None, None, None, None, None, node_parent, None, None, None, None, None)
    insert_node(node_data)
    node_parent = node_id


    next_level = title_name.find_next_siblings("div")
    ## Finding different levels
    for level in next_level:
        class_list = level.get('class', [])

        ## Checking for part
        if level['class'] == "MRSPart_toclist":
            heading = level.find("h2")
            part_name = heading.get_text()

            node_type = "structure"
            node_level_classifier = "PART"
            node_id = f"{node_parent}PART={heading_name}/"
            node_data = (node_id, top_level_title, node_type, node_level_classifier, None, None, None, url, None, heading_name, None, None, None, None, None, node_parent, None, None, None, None, None)
            insert_node(node_data)
            part_node_parent = node_id

            ## Checking for subpart / chapter / section
            next_level = heading.find_next_siblings("div")

            for level in next_level:
                class_list = level.get('class', [])

                if level['class'] == "MRSChapter_toclist":
                    a_tag = level.find("a")
                    url = BASE_URL + "/" + top_level_title + "/" + a_tag['href']
                    chapter_name = heading.get_text()
                    chapter_number_get = chapter_name.split(" ")
                    chapter_number = chapter_number_get[1].replace(":","")
                    node_type = "structure"
                    node_level_classifier = "CHAPTER"
                    node_id = f"{part_node_parent}CHAPTER={chapter_number}/"
                    node_data = (node_id, top_level_title, node_type, node_level_classifier, None, None, None, url, None, chapter_name, None, None, None, None, None, part_node_parent, None, None, None, None, None)
                    insert_node(node_data)
                    chapter_node_parent = node_id
                    scrape_per_chapter(url, top_level_title, chapter_node_parent)
                elif level['class'] == "MRSSubpart_toclist":
                    heading = level.find("h2")
                    heading_name = heading.get_text()
                    ## Check for subpart
                    pass


        elif level['class'] == "MRSChapter_toclist":
            heading = level.find("h2")
            heading_name = heading.get_text()
            ## Check whether chapter or article
            pass
        elif level['class'] == "MRSSubTitle_toclist":
            heading = level.find("h2")
            heading_name = heading.get_text()
            ## Check for part
            pass
        elif "right_nav_repealed" in class_list:
            ## Check for repealed
            heading = level.find("h2")
            heading_name = heading.get_text()
            if "Part" in heading_name:
                pass
            elif "Chapter" in heading_name:
                pass
            elif "Article" in heading_name:
                pass
            elif "Subtitle" in heading_name:
                pass


def scrape_per_chapter(url, top_level_title, node_parent):
    pass
