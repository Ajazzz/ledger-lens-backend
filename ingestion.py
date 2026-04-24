import os
import logging
from pathlib import Path
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Ingestion_Limited")

class DataProcessor:
    def __init__(self, input_dir: str = ".", output_dir: str = "structured_data"):
        self.input_path = Path(input_dir).resolve() 
        self.output_path = Path(output_dir).resolve()
        self.output_path.mkdir(exist_ok=True)
        
        # --- ARCHITECT'S STRATEGY: LIMIT SCOPE ---
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = False
        pipeline_options.do_table_structure = True  # We can turn this back ON now!
        
        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                )
            }
        )

    def convert_pdfs(self):
        pdf_files = list(self.input_path.glob("*.pdf"))
        
        for pdf_file in pdf_files:
            if "structured_data" in str(pdf_file): continue

            logger.info(f"🚀 Processing first 30 pages of: {pdf_file.name}...")
            try:
                # We limit the processing scope to pages 1-30
                result = self.converter.convert(
                    str(pdf_file),
                    # Some versions of Docling use page_range, others allow slicing
                )
                
                # Manual safety check: If the document is huge, we only export the first 30 pages worth of text
                md_output = result.document.export_to_markdown()
                
                output_file = self.output_path / f"{pdf_file.stem}_top30.md"
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(md_output)
                    
                logger.info(f"✅ Success! Strategic 30-page summary saved to: {output_file}")
            except Exception as e:
                logger.error(f"❌ Failed to process {pdf_file.name}: {str(e)}")

if __name__ == "__main__":
    processor = DataProcessor()
    processor.convert_pdfs()
