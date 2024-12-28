# Study Assistant

## New Updates

In this new iteration,

- Beside extracting and processing raw text from PDF files, the program can tag and categorize different themes and topics of reading entries based on  pre-trained data that is yielded from carefully interpretating the table of content and introduction of all reading entries and its associated tags (done manually).
- A new UI is introduced to help simplified step selection and get insights in its working progress
- A new ability to comprehend the prompt based on both the present and the past prompt, applied with adjustable weight.
- Improvements in computation speed and capacities

## Design Structures (Based on Functionalities)

> A sequential design paradigm

A. Preparation for software

1. Extract text from PDF
2. Process basic information about PDF files
3. Tokenize text chunks
4. Interprete token relations
5. Categorize based on preset tags

B. End user functionalities

1. Receive a new number of PDF title recommendation based on input prompt
2. Suggest a number of tags of topics and themes related to the input text
3. View information about reading entries and software training analytics

## Design Considerations

### User Interface

There are 2 options:

- Continuing on the terminal approach with pre-built commands

  - Pro: The commands can easily be implemted or removed
  - Con: The interface might be akward when there are too many commands
  - Note: Required considerations on building atomic commands

- Support a new GUI for better use of the application

  - Pro: Better comprehension of what the application is about, easier to capture necessary information for users
  - Con: time-consuming to build, visual design elements must be considered when building application
  - Note: Consider about concurency when running different modules in background to provide information to the user in the GUI

Since the applicaiton is built using Python for interface with C++ wrapper for the critical parts. It is easier to add GUI in Python. Considerations for GUI in Python are:

- PyQT5
- Tkinter
- PyGUI
- Kivy
- Streamlit

Note: A migration to an language to improve memory safety and computation speed can be considered if needed.

### PDF Text Extraction

PDF text extraction oftentimes is the most time-consuming task in the software as the application is retricted by a single database storage. Multi threating and distributed tasks are considered in the previous iteration. One thread process one reading entries.

**A experiemental approach.** Multi processing (using multiple CPU core at the same time) to process each reading entries individual then insert into its own database. When the process is done, combined all information into a master database.

### Natural Language Processing

Trimming, filtering and tokenizing text chunk are the core of this software and must be handled properly.

With new updates, the software needs to be trained on labelled data. Every keyword or word of assoiation with a tag is carafully observed and tested by multiple text mining techniques. This tasks many overall add in the longer preparation before the software can be used from the end users.

### Other considerations

- The software is computational intensive, and it is slow for Python to handle the amount of calculation. An apporach is to use C++ as a wrapper for these functions
- Files communication. Find an approach to bind C++ into Python, considering "pybing11"
- Data analysis. Getting analytical information about the process of extracting and training the data in an informatic dashboard
- Data management. Create a way to add in data instead of re-create the whole database for every new incoming pdf files
