module FedoraMigrate
  class TargetConstructor
    attr_accessor :source, :candidates, :target

    def initialize(source)
      @source = source
    end

    # Override .build to use Fedora's default id minter
    # https://github.com/projecthydra-labs/fedora-migrate/issues/49
    def build
      raise FedoraMigrate::Errors::MigrationError, "No qualified targets found in #{source.pid}" if target.nil?
      obj = target.new
      obj.apply_depositor_metadata "ebeck01"

      file_set = FileSet.new
      #perform_now(file_set, File.join(mock_upload_directory, 'world.png'), user)

      #.create_content(f)
      obj.title = process_titles

      obj
      # TODO (id: FedoraMigrate::Mover.id_component(source))
      # For now we're scrapping the fedora 3 id in the move but should be addressed.
    end

    def process_titles
      xml = Nokogiri::XML(source.datastreams["DCA-META"].content)
      titles =  xml.xpath("//dc:title")
      target_titles = Array.new
      titles.each do |title|
        (target_titles << title.text) unless title.nil?
      end

      target_titles
    end

    def target
      @target ||= determine_target
    end

    private

      def determine_target
        # MK
        #print "\n\nCandidates\n\n"
        #candidates.each {|candidate| print candidate,"\n"}
        #puts "==="



        Array(candidates).map { |model| vet(model) }.compact.first
      end

      def vet(model)
        # MK
        #print "\n\nVet Model\n\n"
        ##puts "MODEL #{model}"
        #puts "=END="

        FedoraMigrate::Mover.id_component(model).constantize
      rescue NameError
        Logger.debug "rejecting #{model} for target"
        nil
      end

    def candidates
      @candidates = Array(source.models).map do |candidate|
        if candidate == "info:fedora/cm:VotingRecord"
          "TuftsVotingRecord"
        else
          candidate
        end
      end
    end
  end



end

