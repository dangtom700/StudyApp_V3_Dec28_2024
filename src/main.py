import argparse
from datetime import datetime
import modules.path as path
import modules.extract_text as extract_text
import modules.word_freq as word_freq

def get_time_performance(start_time: datetime, message: str) -> None:
    end_time = datetime.now()
    time_diff = end_time - start_time
    print(f"{message} took {time_diff} seconds")

def app():

    parser = argparse.ArgumentParser(prog="Study Logging and Database",
                                     description="This project is to meant to store record of learning activities. The files and record of activities are then transfer into database that show user the timeline and activities done in that day.",
                                     add_help=True,
                                     allow_abbrev=True)
    
    parser.add_argument("--displayHelp", action= 'store_true', help= 'Display help message')
    parser.add_argument("--extractText", action= 'store_true', help= 'Extract text from PDF files and store in database')
    parser.add_argument("--processWordFreq", action= 'store_true', help="Create index tables and analyze word frequencies all in one")
    parser.add_argument("--tokenizePrompt", action= 'store_true', help="Prompt to find references in full database based on context of search")

    args = parser.parse_args()

    if args.displayHelp:
        print("""
              This project is to meant to store record of learning activities. 
              The files and record of activities are then transfer into database 
              that show user the timeline and activities done in that day.

              Python is used to extract text from PDF files and store in database.
              Python also offers a few useful modules to process Natural Language Processing
              and word processing modules to conviniently analyze word frequencies and 
              word stems to clean up textual data for processing cosine similarity search.
              """)

    if args.extractText: # function is functioning properly
        start_time = datetime.now()
        
        # Adjust parameters
        """
        Small Chunks (50-200 characters): These are useful for quick retrieval 
        of specific information, such as definitions or short facts. They are 
        easy to index and search but may lack context.

        Medium Chunks (200-500 characters): Medium chunks are a balance between 
        detail and brevity, providing enough context to understand a concept 
        without overwhelming the reader. These are often used in study aids or 
        summaries.

        Large Chunks (500-2000 characters): Large chunks are better suited for 
        conveying more complex ideas, detailed explanations, or comprehensive 
        descriptions. They are more challenging to search but provide deeper 
        understanding.
        """
        chunk_size = 5000
        # extract_text
        print("Extracting text from PDF files...")
        extract_text.extract_text(CHUNK_SIZE=chunk_size, FOLDER_PATH=path.pdf_path, chunk_database_path=path.chunk_database_path, reset_db=True)
        print("Finished extracting text from PDF files.")
        # announce finish
        get_time_performance(start_time, "Text extracting time")
    
    if args.processWordFreq:
        start_time = datetime.now()

        print("Processing word frequencies...")
        word_freq.process_word_frequencies_in_batches()
        print("Finished processing word frequencies.")
        
        # announce finish
        get_time_performance(start_time, "Word frequency processing time")

    if args.tokenizePrompt: # function is functioning properly
        start_time = datetime.now()
        
        print("Tokenizing prompt...")
        word_freq.promptFindingReference()
        print("Finished tokenizing prompt.")

        # announce finish
        get_time_performance(start_time, "Tokenizing prompt time")

if __name__ == "__main__":
    app()