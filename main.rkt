#lang racket/base
(require racket/file racket/path racket/string)

(provide define-kv-origin kv-ref kv-set! kv-del!  kv-keys)

(define origin (make-parameter #f))

(define (define-kv-origin dir)
  (unless (directory-exists? dir) (make-directory dir))
  (origin dir))

(define (key->path id)
  (unless (origin) (error "undefined origin (define-kv-origin #f)" ))
  (build-path (origin) (string-append (symbol->string id) ".rktd")))

(define (kv-set! id val)
  (call-with-file-lock/timeout
   #:max-delay 200
   #:delay 0.2
   (key->path id)
   'exclusive
   (lambda ()
     (let ([out (open-output-file (key->path id) #:exists 'replace)])
       (write val out)
       (close-output-port out))
     )
   (lambda () (error "Failed to obtain lock for file" (key->path id)))))
 
(define (kv-ref id )
  (call-with-file-lock/timeout
   #:max-delay 200
   #:delay 0.2
   (key->path id)
   'shared
   (lambda () (file->value (key->path id)))
   (lambda () (error "Failed to obtain lock for file" (key->path id)))))
  
  
(define (kv-keys )
  (define (data? a-path)
    (equal? (path-get-extension a-path) #".rktd"))
  (define (file->key a-path)
    (map string->symbol  (string-split (path->string a-path) ".rktd")))
  (map car (map file->key (filter  data? (directory-list (origin))) )))


(define (kv-del! id)
  (call-with-file-lock/timeout
   #:max-delay 200
   #:delay 0.2
   (key->path id)
   'exclusive
   (lambda () (delete-file  (key->path id)))
   (lambda () (error "Failed to obtain lock for file" (key->path id)))))



(module+ test
  (require rackunit)
  (define-kv-origin  (build-path (current-directory) "test"))
  (kv-set! 'key "val")
  (check-true  (file-exists? (build-path (origin) "key.rktd") ))
  (define v1 "val")
  (kv-set! 'key1 v1)
  (check-equal? v1 (kv-ref 'key1))
  (check-true  (file-exists? (build-path (origin) "key1.rktd") ))
  (check-true  (list? (member 'key (kv-keys) )))
  (check-false  (member 'keydsds (kv-keys) ))
  (kv-set! 'val-hash (make-hash '((cu . 10))))
  
  (kv-del! 'key)
  (kv-del! 'key1)
  )
