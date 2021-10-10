let g:ale_fix_on_save = 1

let g:ale_fixers = {'terraform': ['terraform']}
let g:ale_terraform_fmt_executable = 'terraform'

" NERDTree
let g:NERDTreeIgnore = [
 \ '\.git$',
 \ '\.sock$',
 \ '\.pid$',
 \ '\.vim$',
 \ '\.terraform$',
 \ '\.terraform.lock.hcl$',
\ ]

