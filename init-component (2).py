"""UI components package."""

from ui.components.common import (
    set_page_config, render_header, render_footer,
    display_error, display_success, display_info, display_warning,
    create_download_button, format_timestamp
)
from ui.components.sidebar import render_sidebar
from ui.components.chat import render_chat_tab
from ui.components.iep import render_iep_tab
from ui.components.lesson_plan import render_lesson_plan_tab
from ui.components.document_utils import (
    get_available_documents, get_document_by_id, get_document_metadata,
    format_document_preview, display_document_preview
)
from ui.components.visualization import render_analytics_tab

__all__ = [
    'set_page_config',
    'render_header',
    'render_footer',
    'display_error',
    'display_success',
    'display_info',
    'display_warning',
    'create_download_button',
    'format_timestamp',
    'render_sidebar',
    'render_chat_tab',
    'render_iep_tab',
    'render_lesson_plan_tab',
    'get_available_documents',
    'get_document_by_id',
    'get_document_metadata',
    'format_document_preview',
    'display_document_preview',
    'render_analytics_tab'
]
