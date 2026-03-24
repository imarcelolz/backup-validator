 # 1. Setup                                                
  ./setup.sh
  source .venv/bin/activate
                                                                                                                                                                                                                                              
  # 2. Indexar fontes (um por vez, pode interromper e retomar)                                                                                                                                                                                
  python cli.py index /mnt/hd1 --name HD-1 --role source                                                                                                                                                                                      
  python cli.py index /mnt/hd2 --name HD-2 --role source                                                                                                                                                                                      
  python cli.py index /mnt/hd3 --name HD-3 --role source                                                                                                                                                                                      
                                                                                                                                                                                                                                              
  # 3. Indexar consolidado                                                                                                                                                                                                                    
  python cli.py index /mnt/consolidated --name consolidated --role consolidated
                                                                                                                                                                                                                                              
  # 4. Validar integridade (--deep para decode completo de video)                                                                                                                                                                             
  python cli.py check-integrity --source consolidated --deep                                                                                                                                                                                  
                                                                                                                                                                                                                                              
  # 5. Validar arquivo unico                                
  python cli.py validate /mnt/consolidated/video.mp4                                                                                                                                                                                          
                                                                                                                                                                                                                                              
  # 6. Comparar
  python cli.py compare --consolidated consolidated                                                                                                                                                                                           
                                                            
  # 7. Achar copia valida de arquivo corrompido                                                                                                                                                                                               
  python cli.py find-valid-copy /path/to/corrupted.mp4
                                                                                                                                                                                                                                              
  # 8. Relatorio                                            
  python cli.py report --format csv --media-only -o relatorio.csv                                                                                                                                                                             
  python cli.py report --status missing  
