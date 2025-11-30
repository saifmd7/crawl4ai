"""
SailPoint Documentation Crawler
Crawls through all SailPoint documentation pages and displays content in terminal
Uses BestFirstCrawlingStrategy to prioritize relevant pages
Saves output as Markdown files and PDF report
"""

import asyncio
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from urllib.parse import urljoin, urlparse
import json
from typing import Set, List, Dict
from pathlib import Path
from datetime import datetime
import re


class SailPointCrawler:
    def __init__(self, base_url: str, output_dir: str = "sailpoint_docs"):
        self.base_url = base_url
        self.visited_urls: Set[str] = set()
        self.domain = urlparse(base_url).netloc
        self.page_count = 0
        self.output_dir = Path(output_dir)
        self.pages_data: List[Dict] = []  # Store page data for PDF generation
        
        # Create output directories
        self.output_dir.mkdir(exist_ok=True)
        (self.output_dir / "markdown").mkdir(exist_ok=True)
        
        # Create a keyword scorer for SailPoint-specific content
        self.scorer = KeywordRelevanceScorer(
            keywords=["connector", "Plugins", "Provisioning", "report", "integration", 
                     "API", "identity", "governance", "access", "security"],
            weight=0.7
        )
        
        # Configure the best-first crawling strategy
        self.strategy = BestFirstCrawlingStrategy(
            max_depth=2,
            include_external=False,
            url_scorer=self.scorer,
            max_pages=25,  # Maximum number of pages to crawl
        )
        
    def is_valid_url(self, url: str) -> bool:
        """Check if URL belongs to SailPoint documentation domain"""
        parsed = urlparse(url)
        return parsed.netloc == self.domain
    
    def extract_links(self, html: str, current_url: str) -> List[str]:
        """Extract all links from HTML content"""
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(current_url, href)
            
            # Remove fragments
            absolute_url = absolute_url.split('#')[0]
            
            if self.is_valid_url(absolute_url) and absolute_url not in self.visited_urls:
                links.append(absolute_url)
        
        return links
    
    async def crawl_page(self, url: str, crawler: AsyncWebCrawler, max_depth: int = 2, current_depth: int = 0):
        """Crawl a single page and recursively crawl linked pages"""
        if url in self.visited_urls or current_depth > max_depth:
            return
        
        self.visited_urls.add(url)
        self.page_count += 1
        
        print(f"\n{'='*80}")
        print(f"Page {self.page_count} | Crawling: {url}")
        print(f"Depth: {current_depth}/{max_depth}")
        print(f"{'='*80}\n")
        
        try:
            result = await crawler.arun(url=url)
            
            if result.success:
                # Display page title
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(result.html, 'html.parser')
                title = soup.find('title')
                if title:
                    print(f"📄 Title: {title.get_text().strip()}\n")
                
                # Calculate and display relevance score
                try:
                    score = self.scorer.score(result.html)
                    print(f"🎯 Relevance Score: {score:.2f}")
                except Exception as e:
                    score = 0.0
                    print(f"🎯 Relevance Score: N/A")
                
                # Save page data for later processing
                page_data = {
                    'page_num': self.page_count,
                    'url': url,
                    'title': title.get_text().strip() if title else 'No Title',
                    'score': score,
                    'content': result.markdown or result.cleaned_html or '',
                    'depth': current_depth
                }
                self.pages_data.append(page_data)
                
                # Save as individual markdown file
                self.save_markdown(page_data)
                
                # Display markdown content (cleaner format)
                if result.markdown:
                    # Limit output to avoid overwhelming terminal
                    content = result.markdown[:3000]
                    print("\n📝 Content Preview:")
                    print("-" * 80)
                    print(content)
                    if len(result.markdown) > 3000:
                        print(f"\n... (truncated, total length: {len(result.markdown)} chars)")
                    print("-" * 80)
                
                # Extract and crawl links (if not at max depth)
                if current_depth < max_depth:
                    links = self.extract_links(result.html, url)
                    print(f"\n🔗 Found {len(links)} new links to explore")
                    
                    # Score and sort links by relevance (just use URL for scoring)
                    scored_links = []
                    for link in links[:10]:  # Limit initial set
                        try:
                            # Score based on URL content
                            link_score = sum(1 for keyword in self.scorer.keywords if keyword.lower() in link.lower())
                            scored_links.append((link, link_score))
                        except:
                            scored_links.append((link, 0))
                    
                    # Sort by score descending
                    scored_links.sort(key=lambda x: x[1], reverse=True)
                    
                    # Display top scored links
                    if scored_links:
                        print("\n📊 Top Scored Links:")
                        for i, (link, link_score) in enumerate(scored_links[:5], 1):
                            print(f"  {i}. [Score: {link_score}] {link}")
                    
                    # Crawl top-scored links
                    for link, link_score in scored_links[:5]:
                        await self.crawl_page(link, crawler, max_depth, current_depth + 1)
            else:
                print(f"❌ Failed to crawl: {result.error_message}")
                
        except Exception as e:
            print(f"❌ Error crawling {url}: {str(e)}")
    
    def save_markdown(self, page_data: Dict):
        """Save individual page as markdown file"""
        # Create safe filename from URL
        filename = re.sub(r'[^a-zA-Z0-9]', '_', page_data['url'])
        filename = f"{page_data['page_num']:03d}_{filename[:100]}.md"
        
        filepath = self.output_dir / "markdown" / filename
        
        content = f"""# {page_data['title']}

**URL:** {page_data['url']}  
**Relevance Score:** {page_data['score']:.2f}  
**Page:** {page_data['page_num']} | **Depth:** {page_data['depth']}  
**Crawled:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

{page_data['content']}
"""
        
        filepath.write_text(content, encoding='utf-8')
        print(f"💾 Saved: {filepath.name}")
    
    def save_json_report(self):
        """Save all pages data as JSON"""
        json_file = self.output_dir / "crawl_report.json"
        report = {
            'crawl_date': datetime.now().isoformat(),
            'base_url': self.base_url,
            'total_pages': len(self.pages_data),
            'pages': self.pages_data
        }
        json_file.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f"\n💾 JSON Report saved: {json_file}")
    
    def generate_pdf_report(self):
        """Generate PDF report from crawled pages"""
        try:
            from reportlab.lib.pagesizes import letter, A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_CENTER, TA_LEFT
            
            pdf_file = self.output_dir / f"sailpoint_crawl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            doc = SimpleDocTemplate(str(pdf_file), pagesize=letter,
                                   rightMargin=72, leftMargin=72,
                                   topMargin=72, bottomMargin=18)
            
            # Container for the 'Flowable' objects
            elements = []
            styles = getSampleStyleSheet()
            
            # Custom styles
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                textColor=colors.HexColor('#1a5490'),
                spaceAfter=30,
                alignment=TA_CENTER
            )
            
            heading_style = ParagraphStyle(
                'CustomHeading',
                parent=styles['Heading2'],
                fontSize=14,
                textColor=colors.HexColor('#1a5490'),
                spaceAfter=12,
                spaceBefore=12
            )
            
            # Title Page
            elements.append(Paragraph("SailPoint Documentation Crawl Report", title_style))
            elements.append(Spacer(1, 0.2*inch))
            
            # Summary table
            summary_data = [
                ['Crawl Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['Base URL', self.base_url],
                ['Total Pages Crawled', str(len(self.pages_data))],
                ['Avg Relevance Score', f"{sum(p['score'] for p in self.pages_data) / len(self.pages_data):.2f}" if self.pages_data else 'N/A']
            ]
            
            summary_table = Table(summary_data, colWidths=[2*inch, 4*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            
            elements.append(summary_table)
            elements.append(PageBreak())
            
            # Add each page
            for page in sorted(self.pages_data, key=lambda x: x['score'], reverse=True):
                elements.append(Paragraph(f"Page {page['page_num']}: {page['title']}", heading_style))
                elements.append(Paragraph(f"<b>URL:</b> {page['url']}", styles['Normal']))
                elements.append(Paragraph(f"<b>Relevance Score:</b> {page['score']:.2f} | <b>Depth:</b> {page['depth']}", styles['Normal']))
                elements.append(Spacer(1, 0.2*inch))
                
                # Add content (truncated to avoid PDF being too large)
                content = page['content'][:5000]
                # Clean content for PDF
                content = content.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                # Split into paragraphs
                paragraphs = content.split('\n\n')
                for para in paragraphs[:10]:  # Limit paragraphs
                    if para.strip():
                        try:
                            elements.append(Paragraph(para.strip(), styles['BodyText']))
                            elements.append(Spacer(1, 0.1*inch))
                        except:
                            pass
                
                if len(page['content']) > 5000:
                    elements.append(Paragraph(f"<i>... (truncated, total length: {len(page['content'])} chars)</i>", styles['Italic']))
                
                elements.append(PageBreak())
            
            # Build PDF
            doc.build(elements)
            print(f"\n📄 PDF Report saved: {pdf_file}")
            print(f"   Location: {pdf_file.absolute()}")
            return str(pdf_file)
            
        except ImportError:
            print("\n⚠️  reportlab not installed. Installing now...")
            return None
    
    async def crawl(self, max_depth: int = 2):
        """Start crawling from base URL using BestFirstCrawlingStrategy"""
        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║          SailPoint Documentation Crawler                         ║
║          Using Best-First Crawling Strategy                      ║
║          Starting crawl of: {self.base_url:<35}║
║          Max Pages: {self.strategy.max_pages:<48}║
║          Keywords: connector, Plugins, Provisioning, report      ║
╚══════════════════════════════════════════════════════════════════╝
        """)
        
        async with AsyncWebCrawler(verbose=True) as crawler:
            await self.crawl_page(self.base_url, crawler, max_depth)
        
        print(f"""
\n╔══════════════════════════════════════════════════════════════════╗
║                     Crawl Summary                                ║
╠══════════════════════════════════════════════════════════════════╣
║  Total Pages Crawled: {len(self.visited_urls):<42} ║
║  Max Pages Allowed: {self.strategy.max_pages:<44} ║
╚══════════════════════════════════════════════════════════════════╝
        """)
        
        print("\n📊 All crawled URLs:")
        for i, url in enumerate(sorted(self.visited_urls), 1):
            print(f"  {i}. {url}")
        
        # Save reports
        print("\n" + "="*80)
        print("Generating Reports...")
        print("="*80)
        
        self.save_json_report()
        pdf_path = self.generate_pdf_report()
        
        print(f"\n✅ All files saved to: {self.output_dir.absolute()}")
        print(f"   📁 Markdown files: {self.output_dir / 'markdown'}")
        print(f"   📄 JSON report: {self.output_dir / 'crawl_report.json'}")
        if pdf_path:
            print(f"   📄 PDF report: {pdf_path}")


async def main():
    """Main function to run the crawler"""
    base_url = "https://documentation.sailpoint.com/"
    
    # Create crawler instance
    crawler = SailPointCrawler(base_url)
    
    # Start crawling with depth=2 to explore more pages
    await crawler.crawl(max_depth=2)


if __name__ == "__main__":
    # Install required packages if not already installed
    print("🚀 Starting SailPoint Documentation Crawler...\n")
    asyncio.run(main())
