#ifndef STASIS_BUFFERMANAGER_LEGACY_LEGACYBUFFERMANAGER_H
#define STASIS_BUFFERMANAGER_LEGACY_LEGACYBUFFERMANAGER_H
#include <stasis/pageHandle.h>
stasis_buffer_manager_t* stasis_buffer_manager_deprecated_open(stasis_page_handle_t * ph);
stasis_buffer_manager_t* stasis_buffer_manager_deprecated_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt);
#endif//STASIS_BUFFERMANAGER_LEGACY_LEGACYBUFFERMANAGER_H
