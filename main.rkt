#lang racket/base
(require racket/file racket/path racket/string)

(provide kv-origin kv-ref kv-set! kv-del!  kv-keys)

(define kv-origin (make-parameter #f))

(define (kv-origin-check! )
  (unless (kv-origin) (error "undefined kv-origin " ))
  (unless (directory-exists? (kv-origin)) (make-directory* (kv-origin))))
      

(define (key->path id)
  (build-path (kv-origin) (string-append (symbol->string id) ".rktd")))

(define (key->lock id)
  (build-path (kv-origin) (string-append (symbol->string id) ".LOCK")))

(define (kv-set! id val)
  (kv-origin-check!)
  (call-with-file-lock/timeout
   #:max-delay 200
   #:delay 0.2
   #:lock-file (key->lock id)
   (key->path id)
   'exclusive
   (lambda ()
     (let ([out (open-output-file (key->path id) #:exists 'replace)])
       (write val out)
       (close-output-port out))
     )
   (lambda () (error "Failed to obtain lock for file" (key->path id)))))
 
(define (kv-ref id )
  (kv-origin-check!)
  (call-with-file-lock/timeout
   #:max-delay 200
   #:delay 0.2
   #:lock-file (key->lock id)
   (key->path id)
   'shared
   (lambda () (file->value (key->path id)))
   (lambda () (error "Failed to obtain lock for file" (key->path id)))))
  
  
(define (kv-keys )
  (kv-origin-check!)
  (define (data? a-path)
    (equal? (path-get-extension a-path) #".rktd"))
  (define (file->key a-path)
    (map string->symbol  (string-split (path->string a-path) ".rktd")))
  (map car (map file->key (filter  data? (directory-list (kv-origin))) )))


(define (kv-del! id)
  (kv-origin-check!)
  (call-with-file-lock/timeout
   #:max-delay 200
   #:delay 0.2
   #:lock-file (key->lock id)
   (key->path id)
   'exclusive
   (lambda () (delete-file  (key->path id)))
   (lambda () (error "Failed to obtain lock for file" (key->path id)))))



(module+ test
  (require rackunit)
  (kv-origin  (make-temporary-directory))
  (kv-set! 'key "val")
  (check-true  (file-exists? (build-path (kv-origin) "key.rktd") ))
  (define v1 "val")
  (kv-set! 'key1 v1)
  (check-equal? v1 (kv-ref 'key1))
  (check-true  (file-exists? (build-path (kv-origin) "key1.rktd") ))
  (check-true  (list? (member 'key (kv-keys) )))
  (check-false  (member 'keydsds (kv-keys) ))
  (kv-set! 'val-hash (make-hash '((cu . 10))))
  
  (kv-del! 'key)
  (kv-del! 'key1)
  )
